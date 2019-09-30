import sqlalchemy
import pytest

import layabase
import layabase.database_sqlalchemy
from test import DateTimeModuleMock


@pytest.fixture
def controller():
    class TestModel:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    return layabase.CRUDController(TestModel)


@pytest.fixture
def disconnected_database(controller):
    _db = layabase.load("sqlite:///:memory:", [controller])
    _db.metadata.bind.dispose()
    yield _db


def test_get_all_when_db_down(disconnected_database, controller):
    with pytest.raises(Exception) as exception_info:
        controller.get({})
    assert str(exception_info.value) == "Database could not be reached."


def test_get_when_db_down(disconnected_database, controller):
    with pytest.raises(Exception) as exception_info:
        controller.get_one({})
    assert str(exception_info.value) == "Database could not be reached."


def test_add_when_db_down(disconnected_database, controller):
    with pytest.raises(Exception) as exception_info:
        controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert str(exception_info.value) == "Database could not be reached."


def test_update_when_db_down(disconnected_database, controller):
    with pytest.raises(Exception) as exception_info:
        controller.put({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert str(exception_info.value) == "Database could not be reached."


def test_remove_when_db_down(disconnected_database, controller):
    with pytest.raises(Exception) as exception_info:
        controller.delete({})
    assert str(exception_info.value) == "Database could not be reached."


def test_health_details_failure(disconnected_database, monkeypatch):
    monkeypatch.setattr(layabase.database_sqlalchemy, "datetime", DateTimeModuleMock)
    monkeypatch.setattr(
        disconnected_database.metadata.bind.dialect, "do_ping", lambda x: False
    )
    assert layabase.check(disconnected_database) == (
        "fail",
        {
            "sqlite:select": {
                "componentType": "datastore",
                "status": "fail",
                "time": "2018-10-11T15:05:05.663979",
                "output": "Unable to ping database.",
            }
        },
    )


def test_health_details_failure_due_to_exception(disconnected_database, monkeypatch):
    monkeypatch.setattr(layabase.database_sqlalchemy, "datetime", DateTimeModuleMock)

    def raise_exception(*args):
        raise Exception("This is the error")

    monkeypatch.setattr(
        disconnected_database.metadata.bind.dialect, "do_ping", raise_exception
    )
    assert layabase.check(disconnected_database) == (
        "fail",
        {
            "sqlite:select": {
                "componentType": "datastore",
                "status": "fail",
                "time": "2018-10-11T15:05:05.663979",
                "output": "This is the error",
            }
        },
    )
