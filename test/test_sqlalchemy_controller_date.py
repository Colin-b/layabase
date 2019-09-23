import datetime

import pytest
import sqlalchemy

from layabase import database, database_sqlalchemy
from test.flask_restplus_mock import TestAPI


class TestDateController(database.CRUDController):
    pass


def _create_models(base):
    class TestDateModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "date_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        date_str = sqlalchemy.Column(sqlalchemy.Date)
        datetime_str = sqlalchemy.Column(sqlalchemy.DateTime)

    TestDateController.model(TestDateModel)
    return [TestDateModel]


@pytest.fixture
def db():
    _db = database.load("sqlite:///:memory:", _create_models)
    TestDateController.namespace(TestAPI)
    yield _db
    database.reset(_db)


def test_put_is_updating_date(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert (
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        },
        {
            "date_str": "2018-06-01",
            "datetime_str": "1989-12-31T01:00:00",
            "key": "my_key1",
        },
    ) == TestDateController.put(
        {
            "key": "my_key1",
            "date_str": "2018-06-01",
            "datetime_str": "1989-12-31T01:00:00",
        }
    )
    assert [
        {
            "date_str": "2018-06-01",
            "datetime_str": "1989-12-31T01:00:00",
            "key": "my_key1",
        }
    ] == TestDateController.get({"date_str": "2018-06-01"})


def test_get_date_is_handled_for_valid_date(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ] == TestDateController.get({"date_str": datetime.date(2017, 5, 15)})


def test_get_date_is_handled_for_unused_date(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    d = datetime.datetime.strptime("2016-09-23", "%Y-%m-%d").date()
    assert [] == TestDateController.get({"date_str": d})


def test_get_date_is_handled_for_valid_datetime(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ] == TestDateController.get(
        {"datetime_str": datetime.datetime(2016, 9, 23, 23, 59, 59)}
    )


def test_get_date_is_handled_for_unused_datetime(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    dt = datetime.datetime.strptime("2016-09-24T23:59:59", "%Y-%m-%dT%H:%M:%S")
    assert [] == TestDateController.get({"datetime_str": dt})
