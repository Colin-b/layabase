import pytest
import sqlalchemy


import layabase
from layabase.testing import mock_sqlalchemy_health_datetime


@pytest.fixture
def db():
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    return layabase.load("sqlite:///:memory:", [layabase.CRUDController(TestTable)])


def test_health_details(db, mock_sqlalchemy_health_datetime):
    assert layabase.check(db) == (
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


def test_health_details_no_db(db):
    with pytest.raises(Exception) as exception_info:
        layabase.check(None)
    assert "A database connection URL must be provided." == str(exception_info.value)
