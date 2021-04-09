
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
        self.source_worker = Worker(source_dict) 


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


    def run(self):
        src = self.source.source_worker
        logging.getLogger().setLevel(self.loglevel.value)
        logging.getLogger().info(f'Starting Source Manager {self.id}')

        # Do an initial run of the Source
        # Then update the state to Steady

        # Then run the work loop to continually update the sources every 'period'
        while not self.state.should_stop():
            try:
                # Run the source
                logging.getLogger().info(f'Source {src.name} calling acquire')
                data = src.worker.acquire()
                logging.getLogger().info(f'Source {src.name} acquire retuned')
                logging.getLogger().info(f'Source {src.name} filling header')

                ## Process the data block
                if data:
                    t = time.time()
                    header = datablock.Header(self.data_block_t0.channel_manager_id,
                                              create_time=t, creator=src.module)
                    logging.getLogger().info(f'Source {src.name} header done')

                    # TODO: Make sure we can have data dependencies on other sources that affect the data returned from this source
                    # if src.has_other_source_deps()
                    #   other_source_data_block = get_t1_data_block_from_other_source()
                    # data = data.do_dep_based_processing(data, other_source_data_block)

                    self.data_block_put(data, header, self.data_block_t0)
                    logging.getLogger().info(f'Source {src.name} data block put done')
                else:
                    logging.getLogger().warning(f'Source {src.name} acquire retuned no data')
                
                ## Mark that this source has run, and notify those who care
                src.run_counter += 1
                src.data_updated.set()
                logging.getLogger().info(f'Source {src.name} {src.module} finished cycle')

                # If its the first time, update state so that anything waiting for the source to run for the first time proceeds
                if self.state.get() == State.BOOT:
                    self.state.set(State.STEADY)

                # Once we have run the source (be it 1st or nth time) we should wait for the configured interval before running again (shutting down if the signal is raised)
                if src.schedule > 0:
                    stop = src.stop_running.wait(src.schedule)
                    if stop:
                        logging.getLogger().info(f'Received stop_running signal for {src.name}')
                        break
                    else:
                        logging.getLogger().info(f'Source {src.name} runs only once')
                        break

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

    def data_block_put(self, data, header, data_block):
        """
        Put data into data block

        :type data: :obj:`dict`
        :arg data: key, value pairs
        :type header: :obj:`~datablock.Header`
        :arg header: data header
        :type data_block: :obj:`~datablock.DataBlock`
        :arg data_block: data block
        """

        if not isinstance(data, dict):
            logging.getLogger().error(f'data_block put expecting {dict} type, got {type(data)}')
            return
        logging.getLogger().debug(f'data_block_put {data}')
        with data_block.lock:
            metadata = datablock.Metadata(data_block.channel_manager_id,
                                          state='END_CYCLE',
                                          generation_id=data_block.generation_id)
            for key, product in data.items():
                data_block.put(key, product, header, metadata=metadata)
