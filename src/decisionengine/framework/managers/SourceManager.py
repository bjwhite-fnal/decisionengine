
"""
Source Manager
"""
import logging
import time
import multiprocessing

from decisionengine.framework.dataspace import dataspace
from decisionengine.framework.dataspace import datablock
from decisionengine.framework.managers.ComponentManager import create_runner, ComponentManager
from decisionengine.framework.managers.ProcessingState import State
from decisionengine.framework.managers.ProcessingState import ProcessingState

_TRANSFORMS_TO = 300  # 5 minutes
_DEFAULT_SCHEDULE = 300  # ""


class SourceRunner:
    """
    Provides interface to loadable modules and events for synchronization of execution
    """

    def __init__(self, conf_dict):
        """
        :type conf_dict: :obj:`dict`
        :arg conf_dict: configuration dictionary describing the runner
        """

        self.runner = create_runner(conf_dict['module'],
                                     conf_dict['name'],
                                     conf_dict['parameters'])
        self.module = conf_dict['module']
        self.name = self.runner.__class__.__name__
        self.schedule = conf_dict.get('schedule', _DEFAULT_SCHEDULE)
        self.run_counter = 0
        self.data_updated = multiprocessing.Event()
        self.stop_running = multiprocessing.Event()
        logging.getLogger("decision_engine").debug('Creating source execution runner: module=%s name=%s parameters=%s schedule=%s',
                                                   self.module, self.name, conf_dict['parameters'], self.schedule)


class Source:
    """
    Decision Source.
    Instantiates Source runners according to the provided Source configuration
    """

    def __init__(self, name, source_dict):
        """
        :type source_dict: :obj:`dict`
        :arg source_dict: Source configuration
        """
        self.name = name
        self.source_runner = SourceRunner(source_dict)


class SourceManager(ComponentManager):
    """
    Source Manager: Runs decision cycle for transforms and publishers
    """

    def __init__(self, name, generation_id, source_config, global_config):
        super().__init__(name, generation_id, global_config)

        self.source = Source(self.name, source_config)
        self.lock = multiprocessing.Lock()


    def run(self):
        src = self.source.source_runner
        logging.getLogger().setLevel(self.loglevel.value)
        logging.getLogger().info(f'Starting Source Manager {self.id}')

        # Do an initial run of the Source
        # Then update the state to Steady

        # Then run the work loop to continually update the sources every 'period'
        while not self.state.should_stop():
            try:
                # Run the source
                logging.getLogger().info(f'Source {src.name} calling acquire')
                data = src.runner.acquire()
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
        self.take_offline(self.data_block_t0)
        logging.getLogger().info(f'Source {self.name} ({self.id}) is ending its loop.')
