import pytest

from layabase import database, database_mongo
import layabase.testing
from test import DateTimeModuleMock


class TestController(layabase.CRUDController):
    class TestModel:
        __tablename__ = "test_table"

        id = database_mongo.Column()

    model = TestModel


@pytest.fixture
def db():
    _db = database.load("mongomock", [TestController])
    yield _db
    layabase.testing.reset(_db)


def test_health_details_failure(db, monkeypatch):
    monkeypatch.setattr(database_mongo, "datetime", DateTimeModuleMock)

    def fail_ping(*args):
        raise Exception("Unable to ping")

    db.command = fail_ping
    assert database.check(db) == (
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


def test_health_details_success(db, monkeypatch):
    monkeypatch.setattr(database_mongo, "datetime", DateTimeModuleMock)
    assert database.check(db) == (
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
