import json

from bson import Timestamp
import pytest

import layabase
import layabase.mongo
from layabase.testing import mock_mongo_health_datetime


@pytest.fixture
def database():
    class TestCollection:
        __collection_name__ = "test"

        id = layabase.mongo.Column()

    return layabase.load("mongomock", [layabase.CRUDController(TestCollection)])


def test_health_details_failure(database, mock_mongo_health_datetime):
    def fail_ping(*args):
        raise Exception("Unable to ping")

    database.command = fail_ping
    assert layabase.check(database) == (
        "fail",
        {
            "mongomock:ping": {
                "componentType": "datastore",
                "output": "Unable to ping",
                "status": "fail",
                "time": "2018-10-11T15:05:05.663979",
            }
        },
    )


def test_health_details_success(database, mock_mongo_health_datetime):
    assert layabase.check(database) == (
        "pass",
        {
            "mongomock:ping": {
                "componentType": "datastore",
                "observedValue": {"ok": 1.0},
                "status": "pass",
                "time": "2018-10-11T15:05:05.663979",
            }
        },
    )


def test_health_details_json_serializable(database, mock_mongo_health_datetime):
    def extended_ping_info(*args):
        return {
            'ok': 1.0,
            '$clusterTime': {
                'clusterTime': Timestamp(1645111091, 1),
                'signature': {
                    'hash': b'\x12j\x34\x56\xd1\xcb\xf2\xde9\xd9\xfd\xd3\xa2dC\xcbhl8\x12', 'keyId': 1234472483075915777
                }
            },
            'operationTime': Timestamp(1645111091, 1)
        }

    database.command = extended_ping_info
    response = layabase.check(database)

    try:
        json.dumps(response)
    except:
        pytest.fail("MongoDB ping response should be JSON serializable")
