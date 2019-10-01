import pytest
import sqlalchemy


import layabase
import layabase.database_sqlalchemy
from test import DateTimeModuleMock


@pytest.fixture
def db():
    class TestModel:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    return layabase.load("sqlite:///:memory:", [layabase.CRUDController(TestModel)])


def test_health_details(db, monkeypatch):
    monkeypatch.setattr(layabase.database_sqlalchemy, "datetime", DateTimeModuleMock)
    health_status = layabase.check(db)
    expected_result = (
        "pass",
        {
            "sqlite:select": {
                "componentType": "datastore",
                "observedValue": "",
                "status": "pass",
                "time": "2018-10-11T15:05:05.663979",
            }
        },
    )
    assert expected_result == health_status


def test_health_details_no_db(db):
    with pytest.raises(Exception) as exception_info:
        layabase.check(None)
    assert "A database connection URL must be provided." == str(exception_info.value)
