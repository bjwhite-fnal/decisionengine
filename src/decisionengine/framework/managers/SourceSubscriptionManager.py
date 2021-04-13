"""
Source Subscription Manager
"""

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
        self.message_queue = multiprocessing.Queue()

    def subscribe_to_source(self):
        pass

    def unsubscribe_from_source(self):
        pass

    def run(self):
        while self.keep_running.value:
            # listen for updated data product
            # send data products to subscribers
            time.sleep(5)
        self.logger.info('Source Subscription Manager is stopping.')
