
"""
Source Manager
"""
import importlib
import logging
import time
import multiprocessing
import uuid

#import pandas

from decisionengine.framework.dataspace import dataspace
from decisionengine.framework.dataspace import datablock
from decisionengine.framework.managers.ProcessingState import State
from decisionengine.framework.managers.ProcessingState import ProcessingState

_TRANSFORMS_TO = 300  # 5 minutes
_DEFAULT_SCHEDULE = 300  # ""


def _create_worker(module_name, class_name, parameters):
    """
    Create instance of dynamically loaded module
    """
    my_module = importlib.import_module(module_name)
    class_type = getattr(my_module, class_name)
    return class_type(parameters)


class Worker:
    """
    Provides interface to loadable modules and events for synchronization of execution
    """

    def __init__(self, conf_dict):
        """
        :type conf_dict: :obj:`dict`
        :arg conf_dict: configuration dictionary describing the worker
        """

        self.worker = _create_worker(conf_dict['module'],
                                     conf_dict['name'],
                                     conf_dict['parameters'])
        self.module = conf_dict['module']
        self.name = self.worker.__class__.__name__
        self.schedule = conf_dict.get('schedule', _DEFAULT_SCHEDULE)
        self.run_counter = 0
        self.data_updated = multiprocessing.Event()
        self.stop_running = multiprocessing.Event()
        logging.getLogger("decision_engine").debug('Creating source execution worker: module=%s name=%s parameters=%s schedule=%s',
                                                   self.module, self.name, conf_dict['parameters'], self.schedule)


def _make_worker_for(src_config):
    return {name: Worker(e) for name, e in configs.items()}


class Source:
    """
    Decision Source.
    Instantiates Source workers according to the provided Source configuration
    """

    def __init__(self, name, source_dict):
        """
        :type source_dict: :obj:`dict`
        :arg source_dict: Source configuration
        """
        self.name = name
        self.source = Worker(source_dict) 


class SourceManager:
    """
    Source Manager: Runs decision cycle for transforms and publishers
    """

    def __init__(self, name, generation_id, source_config, global_config):
        """
        :type name: :obj:`str`
        :arg name: Name of source corresponding to this source manager
        :type generation_id: :obj:`int`
        :arg generation_id: Source Manager generation id provided by caller
        :type source_config: :obj:`dict`
        :arg source_config: source configuration
        :type global_config: :obj:`dict`
        :arg global_config: global configuration
         """
        self.id = str(uuid.uuid4()).upper()
        self.dataspace = dataspace.DataSpace(global_config)
        self.data_block_t0 = datablock.DataBlock(self.dataspace,
                                                 name,
                                                 self.id,
                                                 generation_id)  # my current data block
        self.name = name
        self.source = Source(self.name, source_config)
        self.state = ProcessingState()
        self.loglevel = multiprocessing.Value('i', logging.WARNING)
        self.lock = multiprocessing.Lock()
        # The rest of this function will go away once the source-proxy
        # has been reimplemented.
        # BJWHITE: I think this should probably go away in lieu of some better startup method
        #for src_worker in self.channel.sources.values():
        #    src_worker.worker.post_create(global_config)


    def run(self):
        logging.getLogger().setLevel(self.loglevel.value)
        logging.getLogger().info(f'Starting Source Manager {self.id}')

        # Do an initial run of the Source
        # Then update the state to Steady

        # Then run the work loop to continually update the sources every 'period'
        while not self.state.should_stop():
            try:
                if self.state.get() == State.BOOT:
                    self.state.set(State.STEADY)
                # Run the source
                # If its the first time, update state so that the BOOT wait proceeds
            except Exception:  # pragma: no cover
                logging.getLogger().exception("Exception in the main loop for a source")
                logging.getLogger().error('Error occured. Source %s exits with state %s',
                                          self.id, self.get_state_name())
                break
        self.state.set(State.OFFLINE)
        logging.getLogger().info(f'Source {self.name} ({self.id}) is ending its loop.')


    def set_loglevel_value(self, log_level):
        """Assumes log_level is a string corresponding to the supported logging-module levels."""
        with self.loglevel.get_lock():
            # Convert from string to int form using technique
            # suggested by logging module
            self.loglevel.value = getattr(logging, log_level)


    def get_state_value(self):
        with self.state.get_lock():
            return self.state.value


    def get_state(self):
        return self.state.get()


    def get_state_name(self):
        return self.get_state().name


    def get_loglevel(self):
        with self.loglevel.get_lock():
            return self.loglevel.value
