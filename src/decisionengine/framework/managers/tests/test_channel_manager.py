import threading
import os
import multiprocessing

import pytest

import decisionengine.framework.config.policies as policies
from decisionengine.framework.config.ValidConfig import ValidConfig
from decisionengine.framework.dataspace.datasources.tests.fixtures import mock_data_block  # noqa: F401
from decisionengine.framework.managers.ChannelManager import ChannelManager, State
from decisionengine.framework.managers.SourceManager import SourceManager
from decisionengine.framework.managers.SourceSubscriptionManager import SourceSubscriptionManager

_CWD = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATH = os.path.join(_CWD, "../../tests/etc/decisionengine")
_CHANNEL_CONFIG_DIR = os.path.join(_CWD, 'channels')

_global_config = ValidConfig(policies.global_config_file(_CONFIG_PATH))

def channel_config(name):
    return ValidConfig(os.path.join(_CHANNEL_CONFIG_DIR, name + '.jsonnet'))

def source_subscription_manager_for():
    return SourceSubscriptionManager()

def source_manager_for(name, source_subscription_manager):
    data_block_queue = source_subscription_manager.data_block_queue
    data_updated = source_subscription_manager.data_updated
    return SourceManager('source1', 0, channel_config(name)['sources']['source1'], _global_config,
        data_block_queue, data_updated)

def channel_manager_for(name, source_subscription_manager):
    current_t0_data_blocks = source_subscription_manager.current_t0_data_blocks
    data_updated = source_subscription_manager.data_updated
    return ChannelManager(name, 1, channel_config(name), _global_config,
        current_t0_data_blocks, data_updated)

class RunSourceSubscriptionManager:
    def __init__(self):
        self._source_subscription_manager = source_subscription_manager_for()
        self._thread = threading.Thread(target=self._source_subscription_manager.run)

    def __enter__(self):
        self._thread.start()
        return self._source_subscription_manager

    def __exit__(self, type, value, traceback):
        if type:
            return False
        self._source_subscription_manager.keep_running = 0
        self._thread.join()

class RunChannel:
    def __init__(self, name, source_subscription_manager):
        self._tm = channel_manager_for(name, source_subscription_manager)
        self._thread = threading.Thread(target=self._tm.run)

    def __enter__(self):
        self._thread.start()
        return self._tm

    def __exit__(self, type, value, traceback):
        if type:
            return False
        self._thread.join()


@pytest.mark.usefixtures("mock_data_block")
def test_channel_manager_construction(mock_data_block):  # noqa: F811
    with RunSourceSubscriptionManager() as source_subscription_manager:
        channel_manager = channel_manager_for('test_channel', source_subscription_manager)
        assert channel_manager.state.has_value(State.BOOT)


@pytest.mark.usefixtures("mock_data_block")
def test_take_channel_manager_offline(mock_data_block):  # noqa: F811
    with RunSourceSubscriptionManager() as source_subscription_manager:
        with RunChannel('test_channel', source_subscription_manager) as channel_manager:
            source_subscription_manager.data_updated['source1'] = True
            channel_manager.state.wait_until(State.STEADY)
            channel_manager.take_offline(None)
            assert channel_manager.state.has_value(State.OFFLINE)


#@pytest.mark.usefixtures("mock_data_block")
#def test_failing_publisher(mock_data_block):  # noqa: F811
#    with RunSourceSubscriptionManager() as source_subscription_manager:
#        source_manager = source_manager_for('failing_publisher', source_subscription_manager)
#        channel_manager = channel_manager_for('failing_publisher', source_subscription_manager)
#        source_manager.run()
#        channel_manager.run()
#        assert channel_manager.state.has_value(State.OFFLINE)
