import os
import pytest

from decisionengine.framework.dataspace.datablock import Header, Metadata
from decisionengine.framework.dataspace.datasources.postgresql import Postgresql, generate_insert_query


@pytest.fixture()
def datasource(postgresql, data):
    with postgresql.cursor() as cursor:
        cwd = os.path.split(os.path.abspath(__file__))[0]
        # Load decision engine schema
        cursor.execute(open(cwd + "/../postgresql.sql", "r").read())
        # Load test data
        for table, rows in data.items():
            for row in rows:
                keys = ",".join(row.keys())
                values = ",".join([f"'{value}'" for value in row.values()])
                cursor.execute(f"INSERT INTO {table} ({keys}) VALUES ({values})")
    postgresql.commit()

    # psycop2ffi implements this a bit differently....
    #  For now, clean up the options to match what we expect
    dsn_info = dict(s.split("=") for s in postgresql.dsn.split())
    dsn_info['password'] = ''
    del dsn_info['options']

    return Postgresql(dsn_info)

@pytest.fixture(scope="session")
def data():
    return {
        "channel_manager": [
            {
                "name": "channel_manager1",
                "component_manager_id": "1"
            },
            {
                "name": "channel_manager2",
                "component_manager_id": "2"
            }
        ],
        "dataproduct": [
            {
                "component_manager_id": "1",
                "generation_id": "1",
                "key": "test_key1",
                "value": "test_value1"
            }
        ]
    }

@pytest.fixture(scope="session")
def channel_manager():
    return {
        "name": "new_channel_manager",
        "component_manager_id": "123"
    }

@pytest.fixture(scope="session")
def dataproduct():
    return {
        "component_manager_id": "1",
        "generation_id": "2",
        "key": "new_key",
        "value": "new_value"
    }

@pytest.fixture(scope="session")
def header(data):
    return Header(
        data["channel_manager"][0]["component_manager_id"]
    )

@pytest.fixture(scope="session")
def metadata(data):
    return Metadata(
        data["channel_manager"][0]["component_manager_id"]
    )

def test_generate_insert_query():
    table_name = "header"
    keys = ["generation_id", "create_time", "creator"]
    expected_query = "INSERT INTO header (generation_id,create_time,creator) VALUES (%s,%s,%s)"

    result_query = generate_insert_query(table_name, keys).strip()

    assert result_query == expected_query

def test_create_tables(datasource):
    assert datasource.create_tables()

def test_store_channel_manager(datasource, channel_manager):
    result = datasource.store_channel_manager(channel_manager["name"], channel_manager["component_manager_id"])
    assert result > 0

def test_get_channel_manager(datasource, channel_manager, data):
    # test valid channel_manager
    result = datasource.get_channel_manager(data["channel_manager"][0]["name"], data["channel_manager"][0]["component_manager_id"])
    assert result["name"] == data["channel_manager"][0]["name"]
    assert result["component_manager_id"] == data["channel_manager"][0]["component_manager_id"]

    result = datasource.get_channel_manager(data["channel_manager"][0]["name"])
    assert result["name"] == data["channel_manager"][0]["name"]

    # test channel_manager not present in the database
    try:
        datasource.get_channel_manager(channel_manager["name"], channel_manager["component_manager_id"])
    except Exception as e:
        assert e.__class__ == KeyError

    try:
        datasource.get_channel_manager(channel_manager["name"])
    except Exception as e:
        assert e.__class__ == KeyError

def test_get_last_generation_id(datasource, channel_manager, data):
    # test valid channel_manager
    result = datasource.get_last_generation_id(data["channel_manager"][0]["name"], data["channel_manager"][0]["component_manager_id"])
    assert result == int(data["dataproduct"][0]["generation_id"])

    result = datasource.get_last_generation_id(data["channel_manager"][0]["name"])
    assert result == int(data["dataproduct"][0]["generation_id"])

    # test channel_manager not present in the database
    try:
        result = datasource.get_last_generation_id(channel_manager["name"], channel_manager["component_manager_id"])
    except Exception as e:
        assert e.__class__ == KeyError

    try:
        datasource.get_last_generation_id(channel_manager["name"])
    except Exception as e:
        assert e.__class__ == KeyError

def test_insert(datasource, dataproduct, header, metadata):
    datasource.insert(dataproduct["component_manager_id"], dataproduct["generation_id"], dataproduct["key"],
                      dataproduct["value"].encode(), header, metadata)
