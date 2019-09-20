import pytest

from layabase import database, database_mongo


def _create_models(base):
    return []


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)

    yield _db

    database.reset(_db)


class DateTimeModuleMock:
    class DateTimeMock:
        @staticmethod
        def utcnow():
            class UTCDateTimeMock:
                @staticmethod
                def isoformat():
                    return "2018-10-11T15:05:05.663979"

            return UTCDateTimeMock

    datetime = DateTimeMock


def test_health_details_failure(db, monkeypatch):
    monkeypatch.setattr(database_mongo, "datetime", DateTimeModuleMock)

    def fail_ping(*args):
        raise Exception("Unable to ping")

    db.command = fail_ping
    assert database.health_details(db) == (
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
    assert database.health_details(db) == (
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
