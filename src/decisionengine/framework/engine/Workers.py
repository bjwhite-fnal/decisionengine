import logging
import multiprocessing
import os
import threading

import decisionengine.framework.managers.ProcessingState as ProcessingState

FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)s - %(module)s - %(process)d - %(threadName)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z")


class Worker(multiprocessing.Process):
    def __init__(self, manager, logger_config):
        super().__init__()
        self.manager = manager
        self.manager_id = manager.id
        self.logger_config = logger_config

    def wait_until(self, state):
        return self.manager.state.wait_until(state)

    def wait_while(self, state):
        return self.manager.state.wait_while(state)

    def get_state_name(self):
        return self.manager.get_state_name()

    def configure_logging(self):
        logger = logging.getLogger()
        logger.setLevel(logging.WARNING)
        logger_rotate_by = self.logger_config.get("file_rotate_by", "size")

        if logger_rotate_by == "size":
            file_handler = logging.handlers.RotatingFileHandler(os.path.join(
                                                                os.path.dirname(
                                                                    self.logger_config["log_file"]),
                                                                self.manager.name + ".log"),
                                                                maxBytes=self.logger_config.get("max_file_size",
                                                                200 * 1000000),
                                                                backupCount=self.logger_config.get("max_backup_count",
                                                                6))
        else:
            file_handler = logging.handlers.TimedRotatingFileHandler(os.path.join(
                                                                     os.path.dirname(
                                                                         self.logger_config["log_file"]),
                                                                     self.manager.name + ".log"),
                                                                     when=self.logger_config.get("rotation_time_unit", 'D'),
                                                                     interval=self.logger_config.get("rotation_time_interval", '1'))

        file_handler.setFormatter(FORMATTER)
        logger.addHandler(file_handler)

        log_level = self.logger_config.get("global_channel_log_level", "WARNING")
        self.manager.set_loglevel_value(log_level)


class SourceWorker(Worker):
    '''
    Class that encapsulates a source's source manager as a separate process.

    This class' run function is called whenever the process is
    started.  If the process is abruptly terminated--e.g. the run
    method is pre-empted by a signal or an os._exit(n) call--the
    Worker object will still exist even if the operating-system
    process no longer does.

    To determine the exit code of this process, use the
    Worker.exitcode value, provided by the multiprocessing.Process
    base class.
    '''

    def __init__(self, source_manager, logger_config):
        super().__init__(source_manager, logger_config)

    def run(self):
        self.configure_logging()
        self.manager.run()


class ChannelWorker(Worker):
    '''
    Class that encapsulates a channel's channel manager as a separate process.

    This class' run function is called whenever the process is
    started.  If the process is abruptly terminated--e.g. the run
    method is pre-empted by a signal or an os._exit(n) call--the
    Worker object will still exist even if the operating-system
    process no longer does.

    To determine the exit code of this process, use the
    Worker.exitcode value, provided by the multiprocessing.Process
    base class.
    '''

    def __init__(self, channel_manager, logger_config):
        super().__init__(channel_manager, logger_config)

    def run(self):
        self.configure_logging()
        self.manager.run()


class Workers:
    '''
    This class manages and provides access to the Worker(s) which own this SourceManager or ChannelManager.

    The intention is that the decision engine never directly interacts with the
    workers but refers to them via a context manager:

      with workers.access() as ws:
          # Access to ws now protected
          ws['new_channel'] = Worker(...)

    In cases where the decision engine's block_while or block_until
    methods must be called (e.g. during tests), one should used the
    unguarded access:

      with workers.unguarded_access() as ws:
          # Access to ws is unprotected
          ws['new_channel'].wait_until(...)

    Calling a blocking method while using the protected context
    manager (i.e. workers.access()) will likely result in a deadlock.
    '''

    def __init__(self):
        self._workers = {}
        self._lock = threading.Lock()

    def _update_worker_states(self):
        with self._lock:
            for name, process in self._workers.items():
                if process.is_alive():
                    continue
                if process.manager.state.inactive():
                    continue
                process.manager.state.set(ProcessingState.State.ERROR)


    class Access:
        def __init__(self, workers, lock):
            self._workers = workers
            self._lock = lock

        def __enter__(self):
            if self._lock:
                self._lock.acquire()
            return self._workers

        def __exit__(self, error, type, bt):
            if self._lock:
                self._lock.release()

    def access(self):
        self._update_worker_states()
        return self.Access(self._workers, self._lock)

    def unguarded_access(self):
        self._update_worker_states()
        return self.Access(self._workers, None)
