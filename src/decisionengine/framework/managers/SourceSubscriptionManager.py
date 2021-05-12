"""
Source Subscription Manager
"""

import logging
import queue
import time
import collections

# The manager will run in a thread
import threading
# But we will use multiprocessing Queues for communication of data products 
import multiprocessing

class Subscription:
    def __init__(self, channel_manager_id, channel_name, sources):
        self.channel_manager_id = channel_manager_id
        self.channel_name = channel_name
        self.sources = sources


class SourceSubscriptionManager(threading.Thread):
    """
    This implements the communication between Sources and Channels
    """
    def __init__(self):
        super().__init__()
        self.keep_running = multiprocessing.Value('i', 1)
        self.data_block_queue = multiprocessing.Queue()
        self.subscribe_queue = multiprocessing.Queue()

        manager = multiprocessing.Manager()
        self.current_t0_data_blocks = manager.dict()
        self.data_updated = manager.dict()
        self.channel_subscribed = manager.dict()

        self.source_subscriptions = collections.defaultdict(list)
        # HOW TO MAP SOURCES TO THEIR IDS FOR SUBSCRIPTION UPDATE?

    def get_new_subscriptions(self):
        try:
            subscription = self.subscribe_queue.get(block=False)
            for source in subscription.sources:
                self.source_subscriptions[source].append(subscription.channel_id)
        except queue.Empty:
            pass

    def run(self):
        while self.keep_running.value:
            # Check for subscriptions
            self.get_new_subscriptions()

            try:
                # Check for new data blocks
                source_new_data = self.data_block_queue.get(timeout=1) # source_new_data = (source_id, (data, header))
                source_id, new_block_info = source_new_data
                self.current_t0_data_blocks[source_id] = new_block_info

                # Update data blocks in DB for channels with new source data blocks
                for channel in self.source_subscriptions[source]:
                    self.update_blocks_for_subscribed_channels()
            except queue.Empty:
                time.sleep(1)
