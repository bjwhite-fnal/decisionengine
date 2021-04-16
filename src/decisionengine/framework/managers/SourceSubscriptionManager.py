"""
Source Subscription Manager
"""

import logging
import queue
import time

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
        self.keep_running = multiprocessing.Value('i', 1)
        self.data_block_queue = multiprocessing.Queue()

        current_manager = multiprocessing.Manager()
        self.current_t0_data_blocks = current_manager.dict()
        self.data_updated = current_manager.dict()

    def run(self):
        while self.keep_running.value:
            try:
                # source_new_data = (source_name, (data, header))
                source_new_data = self.data_block_queue.get(timeout=1)
                source, new_block_info = source_new_data
                self.current_t0_data_blocks[source] = new_block_info
            except queue.Empty:
                pass
