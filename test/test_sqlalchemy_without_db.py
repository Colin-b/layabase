import sqlalchemy
import pytest

from layabase import database, database_sqlalchemy


@pytest.fixture
def db():
    _db = database.load("sqlite:///:memory:", _create_models)
    _db.metadata.bind.dispose()
    _db.metadata.bind.engine.execute = lambda x: exec("raise(Exception(x))")

    yield _db


class SaveModel:
    pass


def _create_models(base):
    class TestModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "sample_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    SaveModel._model = TestModel
    return [TestModel]


def test_get_all_when_db_down(db):
    with pytest.raises(Exception) as exception_info:
        SaveModel._model.get_all()
    assert "Database could not be reached." == str(exception_info.value)


def test_get_when_db_down(db):
    with pytest.raises(Exception) as exception_info:
        SaveModel._model.get()
    assert "Database could not be reached." == str(exception_info.value)


def test_add_when_db_down(db):
    with pytest.raises(Exception) as exception_info:
        SaveModel._model.add(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
    assert "Database could not be reached." == str(exception_info.value)


def test_update_when_db_down(db):
    with pytest.raises(Exception) as exception_info:
        SaveModel._model.update(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
    assert "Database could not be reached." == str(exception_info.value)


def test_remove_when_db_down(db):
    with pytest.raises(Exception) as exception_info:
        SaveModel._model.remove()
    assert "Database could not be reached." == str(exception_info.value)


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
    monkeypatch.setattr(database_sqlalchemy, "datetime", DateTimeModuleMock)
    assert (
        "fail",
        {
            "sqlite:select": {
                "componentType": "datastore",
                "status": "fail",
                "time": "2018-10-11T15:05:05.663979",
                "output": "SELECT 1",
            }
        },
    ) == database.health_details(db)
