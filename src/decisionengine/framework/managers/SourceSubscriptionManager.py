"""
Source Subscription Manager
"""

import logging
import queue
import time
import collections
import threading
import multiprocessing

from decisionengine.framework.dataspace import dataspace
from decisionengine.framework.dataspace import datablock

class Subscription:
    # Subscriptions are used as an abstraction of all the information required by the SourceSubscriptionManager to bind a Channel to its Sources
    def __init__(self, channel_manager_id, channel_manager_name, source_names):
        self.channel_manager_id = channel_manager_id
        self.channel_manager_name = channel_manager_name
        self.sources = source_names


class SourceSubscriptionManager(threading.Thread):
    """
    This implements the communication between Sources and Channels
    """
    def __init__(self, dataspace):
        super().__init__()
        self.dataspace = dataspace
        self.keep_running = multiprocessing.Value('i', 1)
        self.data_block_queue = multiprocessing.Queue() # Incoming data blocks from source processes
        self.subscribe_queue = multiprocessing.Queue() # Incoming source subscriptions by channels

        manager = multiprocessing.Manager()
        self.data_updated = manager.dict() # Map sources to a boolean flag, allowing sources to indicate when they have run
        self.channel_subscribed = manager.dict() # Notify channel when subscription process has completed

        self.source_subscriptions = collections.defaultdict(list) # Map sources to a list of channel ids subscribed to those sources
        self.sources = collections.defaultdict(list) # Map sources to lists of source ids

        self.channel_data_blocks = {} # Map channel ids to channel datablocks, which are used as an interface for writing generated Source data products

    def get_new_subscriptions(self):
        # This thread should construct the datablocks for the channels and manage the source content within them for a channel.i
        #   This prevents us from having to communicate anything more complex than Python builtin classes accross process boundaries.
        generation_id = 0
        try:
            new_subscription = self.subscribe_queue.get(block=False)
            sub_name = new_subscription.channel_manager_name
            sub_id = new_subscription.channel_manager_id
            channel_data_block = datablock.DataBlock(self.dataspace, sub_name, sub_id, generation_id) # Create a DataBlock for per channel dataspace interface access
            self.channel_data_blocks[sub_id] = channel_data_block

            for source in new_subscription.sources:
                self.source_subscriptions[source].append(sub_id)
        except queue.Empty:
            pass
        else:
            self.channel_subscribed[sub_id] = True

    def update_block_for_subscribed_channel(self, channel_id, source_id, source_data, source_header):
        """
        Update the channel data block with the new source data
        """
        # Get the datablock for the channel
        #   Do I need to go to the db and get the most recent to write into? 
        #       Yes... since I need to preserve the other sources (i.e. cache them)
        channel_block = self.channel_data_blocks.get(channel_id)
        assert channel_block is not None

        # Insert the data products for this source into the general channel block
        channel_block_header = datablock.Header(channel_id, create_time=time.time(), creator=source_id)
        with channel_block.lock:
            metadata = datablock.Metadata(channel_block.component_manager_id,
                                          state='END_CYCLE',
                                          generation_id=channel_block.generation_id)
            for key, product in source_data.items():
                channel_block.put(key, product, source_header, metadata=metadata)

                # Now update the channel that the Source has run
                self.data_updated[source_id] = True
        

    def run(self):
        while self.keep_running:
            # Check for subscriptions
            self.get_new_subscriptions()

            try:
                # Check for new data blocks
                # source_new_info = { 'source_name': source_name, 'source_id': source_id , 'data': data, 'header': header }
                new_source_info = self.data_block_queue.get(timeout=1)
            except queue.Empty:
                time.sleep(1)
            else:
                source_name = new_source_info['source_name']
                source_id = new_source_info['source_id']
                data = new_source_info['data']
                header = new_source_info['header']

                # Update data blocks in DB for channels with new source data blocks
                for channel_id in self.source_subscriptions[source_name]:
                    self.update_block_for_subscribed_channel(channel_id, source_id, data, header)
                    # Set data_updated for the source that just ran and was updated so that decision cycles may be started
                    self.data_updated[source_name] = True
