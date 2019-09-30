import sqlalchemy
import pytest

from layabase import database, database_sqlalchemy, CRUDController
from test import DateTimeModuleMock


class TestController(CRUDController):
    class TestModel:
        __tablename__ = "sample_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    model = TestModel


@pytest.fixture
def disconnected_database():
    _db = database.load("sqlite:///:memory:", [TestController])
    _db.metadata.bind.dispose()
    yield _db


def test_get_all_when_db_down(disconnected_database):
    with pytest.raises(Exception) as exception_info:
        TestController.get({})
    assert str(exception_info.value) == "Database could not be reached."


def test_get_when_db_down(disconnected_database):
    with pytest.raises(Exception) as exception_info:
        TestController.get_one({})
    assert str(exception_info.value) == "Database could not be reached."


def test_add_when_db_down(disconnected_database):
    with pytest.raises(Exception) as exception_info:
        TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert str(exception_info.value) == "Database could not be reached."


def test_update_when_db_down(disconnected_database):
    with pytest.raises(Exception) as exception_info:
        TestController.put({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert str(exception_info.value) == "Database could not be reached."


def test_remove_when_db_down(disconnected_database):
    with pytest.raises(Exception) as exception_info:
        TestController.delete({})
    assert str(exception_info.value) == "Database could not be reached."


def test_health_details_failure(disconnected_database, monkeypatch):
    monkeypatch.setattr(database_sqlalchemy, "datetime", DateTimeModuleMock)
    monkeypatch.setattr(
        disconnected_database.metadata.bind.dialect, "do_ping", lambda x: False
    )
    assert database.check(disconnected_database) == (
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
    monkeypatch.setattr(database_sqlalchemy, "datetime", DateTimeModuleMock)

    def raise_exception(*args):
        raise Exception("This is the error")

    monkeypatch.setattr(
        disconnected_database.metadata.bind.dialect, "do_ping", raise_exception
    )
    assert database.check(disconnected_database) == (
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
