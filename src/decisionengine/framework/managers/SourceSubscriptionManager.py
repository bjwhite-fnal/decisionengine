"""
Source Subscription Manager
"""

import time
import queue

# The manager will run in a thread
import threading
# But we will use multiprocessing Queues for communication of data products 
import multiprocessing


class SourceSubscriptionManager(threading.Thread):
    """
    This implements the communication between Sources and Channels
    """
    def __init__(self):
        super().__init__()
        current_manager = multiprocessing.Manager()
        self.current_t0_data_blocks = current_manager.dict()

        self.keep_running = multiprocessing.Value('i', 1)
        self.data_block_queue = multiprocessing.Queue()

    def run(self):
        while self.keep_running.value:
            try:
                source_new_data = self.data_block_queue.get(timeout=1)
            except Queue.Empty:
                pass
            source, new_t0_data_block = source_new_data
            self.current_t0_data_blocks[source] = new_t0_data_block
        self.logger.info('Source Subscription Manager is stopping.')
