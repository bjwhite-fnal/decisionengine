''' Fixture based DE server tests of the ability to start Sources as individual processes managed by the DE server. '''
# pylint: disable=redefined-outer-name

import os
import re
import shutil
import tempfile

import pytest

from decisionengine.framework.engine.DecisionEngine import _get_de_conf_manager, _create_de_server, parse_program_options
from decisionengine.framework.dataspace.datasources.tests.fixtures import mock_data_block  # noqa: F401
from decisionengine.framework.managers.ChannelManager import State
from decisionengine.framework.tests.fixtures import TEST_CONFIG_PATH, TEST_CHANNEL_CONFIG_PATH
from decisionengine.framework.util.sockets import get_random_port

_port = get_random_port()

EMPTY_DIR = tempfile.TemporaryDirectory()

@pytest.fixture
def deserver_mock_data_block(mock_data_block):  # noqa: F811
    global_config, channel_config_handler = _get_de_conf_manager(TEST_CONFIG_PATH,
                                                                 EMPTY_DIR.name,
                                                                 parse_program_options([f'--port={_port}']))
    server = _create_de_server(global_config, channel_config_handler)
    server.start_channels()
    server.block_while(State.BOOT)
    yield server
    server.stop_channels()


def test_start_sources(deserver_mock_data_block, caplog):
    deserver = deserver_mock_data_block

    # Verify that nothing is active
    output = deserver.rpc_status()
    assert "No channels are currently active" in output

    output = deserver.rpc_print_products()
    assert "No channels are currently active" in output

    # Add channel config to directory
    channel_config = os.path.join(TEST_CHANNEL_CONFIG_PATH, 'test_channel.jsonnet')  # noqa: F405
    new_config_path = shutil.copy(channel_config, EMPTY_DIR.name)
    assert os.path.exists(new_config_path)

    # Start the Sources present in the config file
    output = deserver.rpc_start_sources()
    assert output == 'OK' and 'Source source1 started' in caplog.text

    output = deserver.rpc_stop_sources()
    assert output == 'OK' and 'Stopped all sources:' in caplog.text
