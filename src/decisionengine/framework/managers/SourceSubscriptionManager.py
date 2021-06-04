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
    def __init__(self, channel_manager_id, channel_name, sources, channel_data_block):
        self.channel_manager_id = channel_manager_id
        self.channel_name = channel_name
        self.sources = sources
        self.channel_data_block = channel_data_block


class SourceSubscriptionManager(threading.Thread):
    """
    This implements the communication between Sources and Channels
    """
    def __init__(self):
        super().__init__()
        self.keep_running = multiprocessing.Value('i', 1)
        self.data_block_queue = multiprocessing.Queue() # Incoming data blocks from source processes
        self.subscribe_queue = multiprocessing.Queue() # Incoming source subscriptions by channels

        manager = multiprocessing.Manager()
        self.data_updated = manager.dict() # Map sources to a boolean flag, allowing sources to indicate when they have run
        self.channel_subscribed = manager.dict() # Notify channel when subscription process has completed

        self.source_subscriptions = collections.defaultdict(list) # Map sources to a list of channel ids subscribed to those sources
        self.sources = collections.defaultdict(list) # Map sources to lists of source ids

        self.channel_data_blocks = {} # Map channel ids to datablocks for the most version of the datablock

    def get_new_subscriptions(self):
        try:
            subscription = self.subscribe_queue.get(block=False)
            self.channel_data_blocks[subscription.channel_id] = subscription.channel_data_block
            for source in subscription.sources:
                self.source_subscriptions[source].append(subscription.channel_id)
        except queue.Empty:
            pass

    def update_block_for_subscribed_channel(self, channel_id, source_id, source_data, source_header):
        """
        Update the channel data blo
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
                # TODO: There has to be a better way to do this than inserting a new datablock into the DB
                # Might need to modify duplicate() to have a read only mode
                #updated_cache_block = channel_block.duplicate()
                #self.channel_data_blocks[channel_id] = updated_cache_block

                # Now update the channel that the Source has run
        

    def run(self):
        while self.keep_running.value:
            # Check for subscriptions
            self.get_new_subscriptions()

            try:
                # Check for new data blocks
                new_source_info = self.data_block_queue.get(timeout=1) # source_new_data = { 'source_name': source_name, 'source_id': source_id , 'data': data, 'header': header }
            except queue.Empty:
                time.sleep(1)
            else:
                source_name = new_source_info['source_name']
                source_id = new_source_info['source_id']
                data = new_source_info['data']
                header = new_source_info['header']

                # If this source has not been seen yet, add it to the map of source types to ids (Possibly not as needed if the "name" for a source is unique like the id)
                if source_name not in self.sources.keys() \
                    or source_name in self.sources.keys() and source_id not in self.sources[source_name]:
                    self.sources[source_name].append(source_id)
                    
                # Update data blocks in DB for channels with new source data blocks
                for channel_id in self.source_subscriptions[source_name]:
                    self.update_block_for_subscribed_channel(channel_id, source_id, data, header)
