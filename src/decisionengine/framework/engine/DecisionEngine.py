#!/usr/bin/env python3
"""
Main loop for Decision Engine.
The following environment variable points to decision engine configuration file:
``DECISION_ENGINE_CONFIG_FILE``
if this environment variable is not defined the ``DE-Config.py`` file from the ``../tests/etc/` directory will be used.
"""

import argparse
import enum
import importlib
import logging
import signal
import sys
import pandas as pd
import os
import tabulate
import json

import socketserver
import xmlrpc.server

from decisionengine.framework.config import ChannelConfigHandler, ValidConfig, policies
from decisionengine.framework.engine.Workers import SourceWorker, ChannelWorker, Workers
import decisionengine.framework.dataspace.datablock as datablock
import decisionengine.framework.dataspace.dataspace as dataspace
import decisionengine.framework.managers.ProcessingState as ProcessingState
import decisionengine.framework.managers.ChannelManager as ChannelManager
import decisionengine.framework.managers.SourceManager as SourceManager
import decisionengine.framework.managers.SourceSubscriptionManager as SourceSubscriptionManager


class StopState(enum.Enum):
    NotFound = 1
    Clean = 2
    Terminated = 3

def _channel_preamble(name):
    header = f'Channel: {name}'
    rule = '=' * len(header)
    return '\n' + rule + '\n' + header + '\n' + rule + '\n\n'


class RequestHandler(xmlrpc.server.SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


class DecisionEngine(socketserver.ThreadingMixIn,
                     xmlrpc.server.SimpleXMLRPCServer):

    def __init__(self, global_config, channel_config_loader, server_address):
        xmlrpc.server.SimpleXMLRPCServer.__init__(self,
                                                  server_address,
                                                  logRequests=False,
                                                  requestHandler=RequestHandler)

        self.logger = logging.getLogger("decision_engine")
        signal.signal(signal.SIGHUP, self.handle_sighup)
        self.source_workers = Workers()
        self.channel_workers = Workers()
        self.channel_config_loader = channel_config_loader
        self.global_config = global_config
        self.dataspace = dataspace.DataSpace(self.global_config)
        self.reaper = dataspace.Reaper(self.global_config)
        self.logger.info("DecisionEngine started on {}".format(server_address))

        self.source_subscription_manager = SourceSubscriptionManager.SourceSubscriptionManager(self.dataspace)
        self.source_subscription_manager.start()
        self.logger.info("DecisionEngine SourceSubscriptionManager started in thread {}".format(self.source_subscription_manager.ident))

    def get_logger(self):
        return self.logger

    def _dispatch(self, method, params):
        try:
            # methods allowed to be executed by rpc have 'rpc_' pre-pended
            func = getattr(self, "rpc_" + method)
        except AttributeError:
            raise Exception(f'method "{method}" is not supported')
        return func(*params)

    def block_until(self, state):
        with self.channel_workers.unguarded_access() as workers:
            if not workers:
                self.logger.info('No active channels.')
            for cm in workers.values():
                if cm.is_alive():
                    cm.wait_until(state)

    def block_while(self, state):
        with self.channel_workers.unguarded_access() as workers:
            if not workers:
                self.logger.info('No active channels.')
            for cm in workers.values():
                if cm.is_alive():
                    cm.wait_while(state)

    def rpc_block_while(self, state_str):
        allowed_state = None
        try:
            allowed_state = ProcessingState.State[state_str]
        except Exception:
            return f'{state_str} is not a valid channel state.'
        self.block_while(allowed_state)
        return f'No channels in {state_str} state.'

    def rpc_show_config(self, channel):
        """
        Show the configuration for a channel.

        :type channel: string
        """
        txt = ""
        channels = self.channel_config_loader.get_channels()
        if channel == 'all':
            for ch in channels:
                txt += _channel_preamble(ch)
                txt += self.channel_config_loader.print_channel_config(ch)
            return txt

        if channel not in channels:
            return f"There is no active channel named {channel}."

        txt += _channel_preamble(channel)
        txt += self.channel_config_loader.print_channel_config(channel)
        return txt

    def rpc_show_de_config(self):
        return self.global_config.dump()

    def rpc_print_product(self, product, columns=None, query=None, types=False, format=None):
        def dataframe_to_table(df):
            return "{}\n".format(tabulate.tabulate(df, headers='keys', tablefmt='psql'))

        def dataframe_to_vertical_tables(df):
            txt = ""
            for i in range(len(df)):
                txt += f"Row {i}\n"
                txt += "{}\n".format(tabulate.tabulate(df.T.iloc[:, [i]], tablefmt='psql'))
            return txt

        def dataframe_to_column_names(df):
            columns = df.columns.values.reshape([len(df.columns), 1])
            return "{}\n".format(tabulate.tabulate(columns, headers=['columns'], tablefmt='psql'))

        def dataframe_to_json(df):
            return "{}\n".format(json.dumps(json.loads(df.to_json()), indent=4))

        found = False
        txt = "Product {}: ".format(product)
        with self.channel_workers.access() as workers:
            for ch, worker in workers.items():
                if not worker.is_alive():
                    txt += f"Channel {ch} is in not active\n"
                    continue

                channel_config = self.channel_config_loader.get_channels()[ch]
                produces = self.channel_config_loader.get_produces(channel_config)
                r = [x for x in list(produces.items()) if product in x[1]]
                if not r:
                    continue
                found = True
                txt += " Found in channel {}\n".format(ch)
                cm = self.dataspace.get_channel_manager(ch)
                import pdb; pdb.set_trace()
                try:
                    data_block = datablock.DataBlock(self.dataspace,
                                                     ch,
                                                     component_manager_id=cm['component_manager_id'],
                                                     sequence_id=cm['sequence_id'])
                    data_block.generation_id -= 1
                    df = data_block[product]
                    df = pd.read_json(df.to_json())
                    dataframe_formatter = dataframe_to_table
                    if format == 'vertical':
                        dataframe_formatter = dataframe_to_vertical_tables
                    if format == 'column-names':
                        dataframe_formatter = dataframe_to_column_names
                    if format == 'json':
                        dataframe_formatter = dataframe_to_json
                    if types:
                        for column in df.columns:
                            df.insert(
                                df.columns.get_loc(column) + 1,
                                f"{column}.type",
                                df[column].transform(lambda x: type(x).__name__)
                            )
                    column_names = []
                    if columns:
                        column_names = columns.split(",")
                    if query:
                        if column_names:
                            txt += dataframe_formatter(df.loc[:, column_names].query(query))
                        else:
                            txt += dataframe_formatter(df.query(query))

                    else:
                        if column_names:
                            txt += dataframe_formatter(df.loc[:, column_names])
                        else:
                            txt += dataframe_formatter(df)
                except Exception as e:
                    txt += "\t\t{}\n".format(e)
        if not found:
            txt += "Not produced by any module\n"
        return txt[:-1]

    def rpc_print_products(self):
        with self.channel_workers.access() as workers:
            channel_keys = workers.keys()
            if not channel_keys:
                return "No channels are currently active.\n"

            width = max([len(x) for x in channel_keys]) + 1
            txt = ""
            for ch, worker in workers.items():
                if not worker.is_alive():
                    txt += f"Channel {ch} is in ERROR state\n"
                    continue

                txt += "channel: {:<{width}}, id = {:<{width}}, state = {:<10} \n".format(ch,
                                                                                          worker.manager_id,
                                                                                          worker.get_state_name(),
                                                                                          width=width)
                cm = self.dataspace.get_channel_manager(ch)
                data_block = datablock.DataBlock(self.dataspace,
                                                 ch,
                                                 component_manager_id=cm['component_manager_id'],
                                                 sequence_id=cm['sequence_id'])
                data_block.generation_id -= 1
                channel_config = self.channel_config_loader.get_channels()[ch]
                produces = self.channel_config_loader.get_produces(channel_config)
                for i in ("sources",
                          "transforms",
                          "logicengines",
                          "publishers"):
                    txt += "\t{}:\n".format(i)
                    modules = channel_config.get(i, {})
                    for mod_name, mod_config in modules.items():
                        txt += "\t\t{}\n".format(mod_name)
                        products = produces.get(mod_name, [])
                        for product in products:
                            try:
                                df = data_block[product]
                                df = pd.read_json(df.to_json())
                                txt += "{}\n".format(tabulate.tabulate(df,
                                                                       headers='keys', tablefmt='psql'))
                            except Exception as e:
                                txt += "\t\t\t{}\n".format(e)
        return txt[:-1]

    def rpc_status(self):
        with self.channel_workers.access() as workers:
            channel_keys = workers.keys()
            if not channel_keys:
                return "No channels are currently active.\n" + self.reaper_status()

            txt = ""
            width = max([len(x) for x in channel_keys]) + 1
            for ch, worker in workers.items():
                txt += "channel: {:<{width}}, id = {:<{width}}, state = {:<10} \n".format(ch,
                                                                                          worker.manager_id,
                                                                                          worker.get_state_name(),
                                                                                          width=width)
                channel_config = self.channel_config_loader.get_channels()[ch]
                for i in ("sources",
                          "transforms",
                          "logicengines",
                          "publishers"):
                    txt += "\t{}:\n".format(i)
                    modules = channel_config.get(i, {})
                    for mod_name, mod_config in modules.items():
                        txt += "\t\t{}\n".format(mod_name)
                        my_module = importlib.import_module(
                            mod_config.get('module'))
                        produces = None
                        consumes = None
                        try:
                            produces = getattr(my_module, 'PRODUCES')
                        except AttributeError:
                            pass
                        try:
                            consumes = getattr(my_module, 'CONSUMES')
                        except AttributeError:
                            pass
                        txt += "\t\t\tconsumes : {}\n".format(consumes)
                        txt += "\t\t\tproduces : {}\n".format(produces)
        return txt + self.reaper_status()

    def rpc_stop(self):
        self.shutdown()
        self.stop_sources()
        self.stop_channels()
        self.reaper_stop()
        return "OK"

    def start_source(self, source_name, source_config):
        generation_id = 0
        source_manager = SourceManager.SourceManager(source_name,
                                               generation_id,
                                               source_config,
                                               self.global_config,
                                               self.source_subscription_manager.data_block_queue,
                                               self.source_subscription_manager.data_updated)
        worker = SourceWorker(source_manager, self.global_config['logger'])
        with self.source_workers.access() as workers:
            workers[source_name] = worker
        self.logger.debug(f"Trying to start {source_name}")
        worker.start()
        worker.wait_while(ProcessingState.State['BOOT'])
        self.logger.info(f"Source {source_name} started")

    def start_sources(self):
        self.channel_config_loader.load_all_channels()

        if not self.channel_config_loader.get_channels():
            self.logger.info("No channel configurations available in " +
                             f"{self.channel_config_loader.channel_config_dir}")

        all_source_configs = { name : config['sources'] for (name,config) in self.channel_config_loader.get_channels().items() }

        for channel, channel_source_configs in all_source_configs.items():
            for source_name, source_config in channel_source_configs.items():
                try:
                    self.start_source(source_name, source_config)
                except Exception as e:
                    self.logger.error(f"Source {source_name} failed to start : {e}")

    def rpc_start_source(self, source_name):
        # TODO: This isn't gonna be happy about the config we are using here
        #   needs to be the source name/config or something like that instead
        with self.source_workers.access() as workers:
            if source_name in workers:
                return f"ERROR, source {source_name} is running"

            success, result = self.channel_config_loader.load_channel(source_name)
            if not success:
                return result
            self.start_source(source_name, result)
        return "OK"

    def rpc_start_sources(self):
        self.start_sources()
        return "OK"

    def stop_sources(self, timeout=None):
        if timeout is None:
            timeout = self.global_config.get("shutdown_timeout", 10)

        self.source_subscription_manager.keep_running.value = 0 # Boolean ctype primitives do not exist
        self.source_subscription_manager.join()
        self.logger.info('Stopped Source Subscription Manager.')
        with self.source_workers.access() as workers:
            for source_name, source_worker in workers.items():
                self.stop_worker(source_worker, timeout)
            self.logger.info('Stopped all sources: %s' % workers.keys())

    def rpc_stop_sources(self, timeout=None):
        self.stop_sources(timeout)
        return 'OK'

    def rpc_stop_source(self):
        raise NotImplementedError

    def rpc_kill_source(self, source, timeout=None):
        raise NotImplementedError

    def rm_source(self, source, maybe_timeout):
        raise NotImplementedError

    def rpc_rm_source(self, source, maybe_timeout):
        raise NotImplementedError

    def start_channel(self, channel_name, channel_config):
        generation_id = 1
        channel_manager = ChannelManager.ChannelManager(channel_name,
                                               generation_id,
                                               channel_config,
                                               self.global_config,
                                               self.source_subscription_manager.data_updated,
                                               self.source_subscription_manager.subscribe_queue,
                                               self.source_subscription_manager.channel_subscribed)
        worker = ChannelWorker(channel_manager, self.global_config['logger'])
        with self.channel_workers.access() as workers:
            workers[channel_name] = worker
        self.logger.debug(f"Trying to start {channel_name}")
        worker.start()
        worker.wait_while(ProcessingState.State['BOOT'])
        self.logger.info(f"Channel {channel_name} started")

    def start_channels(self):
        self.channel_config_loader.load_all_channels()

        if not self.channel_config_loader.get_channels():
            self.logger.info("No channel configurations available in " +
                             f"{self.channel_config_loader.channel_config_dir}")

        for name, config in self.channel_config_loader.get_channels().items():
            try:
                self.start_channel(name, config)
            except Exception as e:
                self.logger.error(f"Channel {name} failed to start : {e}")

    def rpc_start_channel(self, channel_name):
        with self.channel_workers.access() as workers:
            if channel_name in workers:
                return f"ERROR, channel {channel_name} is running"

        success, result = self.channel_config_loader.load_channel(channel_name)
        if not success:
            return result
        self.start_channel(channel_name, result)
        return "OK"

    def rpc_start_channels(self):
        self.start_channels()
        return "OK"

    def rpc_stop_channel(self, channel):
        return self.rpc_rm_channel(channel, None)

    def rpc_kill_channel(self, channel, timeout=None):
        if timeout is None:
            timeout = self.global_config.get("shutdown_timeout", 10)
        return self.rpc_rm_channel(channel, timeout)

    def rpc_rm_channel(self, channel, maybe_timeout):
        rc = self.rm_channel(channel, maybe_timeout)
        if rc == StopState.NotFound:
            return f"No channel found with the name {channel}."
        elif rc == StopState.Terminated:
            if maybe_timeout == 0:
                return f"Channel {channel} has been killed."
            # Would be better to use something like the inflect
            # module, but that introduces another dependency.
            suffix = 's' if maybe_timeout > 1 else ''
            return f"Channel {channel} has been killed due to shutdown timeout ({maybe_timeout} second{suffix})."
        assert rc == StopState.Clean
        return f"Channel {channel} stopped cleanly."

    def rm_channel(self, channel, maybe_timeout):
        rc = None
        with self.channel_workers.access() as workers:
            if channel not in workers:
                return StopState.NotFound
            self.logger.debug(f"Trying to stop {channel}")
            rc = self.stop_worker(workers[channel], maybe_timeout)
            del workers[channel]
        return rc

    def stop_worker(self, worker, timeout):
        if worker.is_alive():
            worker.manager.take_offline(None)
            worker.join(timeout)
        if worker.exitcode is None:
            worker.terminate()
            return StopState.Terminated
        else:
            return StopState.Clean

    def stop_channels(self):
        timeout = self.global_config.get("shutdown_timeout", 10)
        with self.channel_workers.access() as workers:
            for worker in workers.values():
                self.stop_worker(worker, timeout)
            workers.clear()

    def rpc_stop_channels(self):
        self.stop_channels()
        return "All channels stopped."

    def handle_sighup(self, signum, frame):
        self.reaper_stop()
        self.stop_channels()
        self.start_channels()
        self.reaper_start(delay=self.global_config['dataspace'].get('reaper_start_delay_seconds', 1818))

    def rpc_get_log_level(self):
        engineloglevel = self.get_logger().getEffectiveLevel()
        return logging.getLevelName(engineloglevel)

    def rpc_get_channel_log_level(self, channel):
        with self.channel_workers.access() as workers:
            if channel not in workers:
                return f"No channel found with the name {channel}."

            worker = workers[channel]
            if not worker.is_alive():
                return f"Channel {channel} is in ERROR state."
            return logging.getLevelName(worker.manager.get_loglevel())

    def rpc_set_channel_log_level(self, channel, log_level):
        """Assumes log_level is a string corresponding to the supported logging-module levels."""
        with self.channel_workers.access() as workers:
            if channel not in workers:
                return f"No channel found with the name {channel}."

            worker = workers[channel]
            if not worker.is_alive():
                return f"Channel {channel} is in ERROR state."

            log_level_code = getattr(logging, log_level)
            if worker.manager.get_loglevel() == log_level_code:
                return f"Nothing to do. Current log level is : {log_level}"
            worker.manager.set_loglevel_value(log_level)
        return f"Log level changed to : {log_level}"

    def rpc_reaper_start(self, delay=0):
        '''
        Start the reaper process after 'delay' seconds.
        Default 0 seconds delay.
        :type delay: int
        '''
        self.reaper_start(delay)
        return "OK"

    def reaper_start(self, delay):
        self.reaper.start(delay)

    def rpc_reaper_stop(self):
        self.reaper_stop()
        return "OK"

    def reaper_stop(self):
        self.reaper.stop()

    def rpc_reaper_status(self):
        interval = self.reaper.get_retention_interval()
        state = self.reaper.get_state()
        txt = 'reaper:\n\tstate: {}\n\tretention_interval: {}'.format(state, interval)
        return txt

    def reaper_status(self):
        interval = self.reaper.get_retention_interval()
        state = self.reaper.get_state()
        txt = '\nreaper:\n\tstate: {}\n\tretention_interval: {}\n'.format(state, interval)
        return txt


def parse_program_options(args=None):
    ''' If args is a list, it will be used instead of sys.argv '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--port",
                        default=8888,
                        type=int,
                        choices=range(1, 65535),
                        metavar="<port number>",
                        help="Default port number is 8888; allowed values are in the half-open interval [1, 65535).")
    parser.add_argument("--config",
                        default=policies.GLOBAL_CONFIG_FILENAME,
                        metavar="<filename>",
                        help="Configuration file for initializing server; default behavior is to choose " +
                        f"'{policies.GLOBAL_CONFIG_FILENAME}' located in the CONFIG_PATH directory.")
    return parser.parse_args(args)


def _get_global_config(config_file, options):
    global_config = None
    try:
        global_config = ValidConfig.ValidConfig(config_file)
    except Exception as msg:
        sys.exit(f"Failed to load configuration {config_file}\n{msg}")

    global_config.update({
        'server_address': ['localhost', options.port]  # Use Jsonnet-supported schema (i.e. not a tuple)
    })
    return global_config


def _get_de_conf_manager(global_config_dir, channel_config_dir, options):
    config_file = os.path.join(global_config_dir, options.config)
    if not os.path.isfile(config_file):
        raise Exception(f"Config file '{config_file}' not found")

    global_config = _get_global_config(config_file, options)
    conf_manager = ChannelConfigHandler.ChannelConfigHandler(global_config, channel_config_dir)

    return (global_config, conf_manager)


def _create_de_server(global_config, channel_config_loader):
    '''Create the DE server with the passed global configuration and config manager'''
    server_address = tuple(global_config.get('server_address'))
    return DecisionEngine(global_config, channel_config_loader, server_address)


def _start_de_server(global_config, channel_config_loader):
    '''Create and start the DE server with the passed global configuration and config manager'''
    try:
        server = _create_de_server(global_config, channel_config_loader)
        server.reaper_start(delay=global_config['dataspace'].get('reaper_start_delay_seconds', 1818))
        server.start_channels()
        server.serve_forever()
    except Exception as e:  # pragma: no cover
        msg = f"""Server Address: {global_config.get('server_address')}
              Fatal Error: {e}"""
        print(msg, file=sys.stderr)

        try:
            server.get_logger().error(msg)
        except Exception:
            pass

        raise e



def main(args=None):
    '''
    If args is None, sys.argv will be used instead
    If args is a list, it will be used instead of sys.argv (for unit testing)
    '''
    options = parse_program_options(args)
    global_config_dir = policies.global_config_dir()
    global_config, channel_config_loader = _get_de_conf_manager(global_config_dir,
                                                                policies.channel_config_dir(),
                                                                options)
    try:
        _start_de_server(global_config, channel_config_loader)
    except Exception as e:  # pragma: no cover
        msg = f"""Config Dir: {global_config_dir}
              Fatal Error: {e}"""
        print(msg, file=sys.stderr)
        sys.exit(msg)


if __name__ == "__main__":
    main()
