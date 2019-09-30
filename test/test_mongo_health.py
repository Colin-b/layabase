import pytest

import layabase
import layabase.database_mongo
import layabase.testing
from test import DateTimeModuleMock


@pytest.fixture
def database():
    class TestController(layabase.CRUDController):
        class TestModel:
            __tablename__ = "test_table"

            id = layabase.database_mongo.Column()

        model = TestModel

    _db = layabase.load("mongomock", [TestController])
    yield _db
    layabase.testing.reset(_db)


def test_health_details_failure(database, monkeypatch):
    monkeypatch.setattr(layabase.database_mongo, "datetime", DateTimeModuleMock)

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


def test_health_details_success(database, monkeypatch):
    monkeypatch.setattr(layabase.database_mongo, "datetime", DateTimeModuleMock)
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
