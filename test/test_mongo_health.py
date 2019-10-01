import pytest

import layabase
import layabase.database_mongo
from layabase.testing import mock_mongo_health_datetime


@pytest.fixture
def database():
    class TestModel:
        __tablename__ = "test"

        id = layabase.database_mongo.Column()

    return layabase.load("mongomock", [layabase.CRUDController(TestModel)])


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
