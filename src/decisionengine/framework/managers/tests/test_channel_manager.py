import threading
import os
import multiprocessing

import pytest

import decisionengine.framework.config.policies as policies
from decisionengine.framework.config.ValidConfig import ValidConfig
from decisionengine.framework.dataspace.datasources.tests.fixtures import mock_data_block  # noqa: F401
from decisionengine.framework.managers.ChannelManager import ChannelManager, State

_CWD = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATH = os.path.join(_CWD, "../../tests/etc/decisionengine")
_CHANNEL_CONFIG_DIR = os.path.join(_CWD, 'channels')

_global_config = ValidConfig(policies.global_config_file(_CONFIG_PATH))
_manager = multiprocessing.Manager()
_data_updated = _manager.dict()
_current_t0_data_blocks = multiprocessing.Queue()


def channel_config(name):
    return ValidConfig(os.path.join(_CHANNEL_CONFIG_DIR, name + '.jsonnet'))


def channel_manager_for(name):
    return ChannelManager(name, 1, channel_config(name), _global_config,
        _current_t0_data_blocks, _data_updated)


class RunChannel:
    def __init__(self, name):
        self._tm = channel_manager_for(name)
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
    channel_manager = channel_manager_for('test_channel')
    assert channel_manager.state.has_value(State.BOOT)


@pytest.mark.usefixtures("mock_data_block")
def test_take_channel_manager_offline(mock_data_block):  # noqa: F811
    with RunChannel('test_channel') as channel_manager:
        _data_updated['source1'] = True
        channel_manager.state.wait_until(State.STEADY)
        channel_manager.take_offline(None)
        assert channel_manager.state.has_value(State.OFFLINE)


@pytest.mark.usefixtures("mock_data_block")
def test_failing_publisher(mock_data_block):  # noqa: F811
    channel_manager = channel_manager_for('failing_publisher')
    channel_manager.run()
    assert channel_manager.state.has_value(State.OFFLINE)
