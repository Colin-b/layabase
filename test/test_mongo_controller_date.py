import datetime

import pytest
from layaberr import ValidationFailed, ModelCouldNotBeFound

from layabase import database, database_mongo
from test.flask_restplus_mock import TestAPI


class TestDateController(database.CRUDController):
    pass


def _create_models(base):
    class TestDateModel(
        database_mongo.CRUDModel, base=base, table_name="date_table_name"
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        date_str = database_mongo.Column(datetime.date)
        datetime_str = database_mongo.Column(datetime.datetime)

    TestDateController.model(TestDateModel)

    return [TestDateModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
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
    d = datetime.datetime.strptime("2017-05-15", "%Y-%m-%d").date()
    assert [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ] == TestDateController.get({"date_str": d})


def test_post_invalid_date_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "this is not a date",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
    assert {"date_str": ["Not a valid date."]} == exception_info.value.errors
    assert {
        "key": "my_key1",
        "date_str": "this is not a date",
        "datetime_str": "2016-09-23T23:59:59",
    } == exception_info.value.received_data


def test_get_invalid_date_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDateController.get({"date_str": "this is not a date"})
    assert {"date_str": ["Not a valid date."]} == exception_info.value.errors
    assert {"date_str": "this is not a date"} == exception_info.value.received_data


def test_delete_invalid_date_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDateController.delete({"date_str": "this is not a date"})
    assert {"date_str": ["Not a valid date."]} == exception_info.value.errors
    assert {"date_str": "this is not a date"} == exception_info.value.received_data


def test_get_with_unknown_fields_is_valid(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert [
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    ] == TestDateController.get({"date_str": "2018-12-30", "unknown_field": "value"})


def test_put_with_unknown_fields_is_valid(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert (
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        },
        {
            "key": "my_key1",
            "date_str": "2018-12-31",
            "datetime_str": "2016-09-23T23:59:59",
        },
    ) == TestDateController.put(
        {"key": "my_key1", "date_str": "2018-12-31", "unknown_field": "value"}
    )
    assert [
        {
            "key": "my_key1",
            "date_str": "2018-12-31",
            "datetime_str": "2016-09-23T23:59:59",
        }
    ] == TestDateController.get({"date_str": "2018-12-31"})
    assert [] == TestDateController.get({"date_str": "2018-12-30"})


def test_put_unexisting_is_invalid(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    with pytest.raises(ModelCouldNotBeFound) as exception_info:
        TestDateController.put({"key": "my_key2"})
    assert {"key": "my_key2"} == exception_info.value.requested_data


def test_post_invalid_datetime_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "2016-09-23",
                "datetime_str": "This is not a valid datetime",
            }
        )
    assert {"datetime_str": ["Not a valid datetime."]} == exception_info.value.errors
    assert {
        "key": "my_key1",
        "date_str": "2016-09-23",
        "datetime_str": "This is not a valid datetime",
    } == exception_info.value.received_data


def test_post_datetime_for_a_date_is_valid(db):
    assert {
        "key": "my_key1",
        "date_str": "2017-05-01",
        "datetime_str": "2017-05-30T01:05:45",
    } == TestDateController.post(
        {
            "key": "my_key1",
            "date_str": datetime.datetime.strptime(
                "2017-05-01T01:05:45", "%Y-%m-%dT%H:%M:%S"
            ),
            "datetime_str": "2017-05-30T01:05:45",
        }
    )


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
    dt = datetime.datetime.strptime("2016-09-23T23:59:59", "%Y-%m-%dT%H:%M:%S")
    assert [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ] == TestDateController.get({"datetime_str": dt})


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


def test_get_response_model_with_date(db):
    assert "TestDateModel" == TestDateController.get_response_model.name
    assert {
        "date_str": "Date",
        "datetime_str": "DateTime",
        "key": "String",
    } == TestDateController.get_response_model.fields_flask_type
    assert {
        "date_str": None,
        "datetime_str": None,
        "key": None,
    } == TestDateController.get_response_model.fields_description
    assert {
        "date_str": None,
        "datetime_str": None,
        "key": None,
    } == TestDateController.get_response_model.fields_enum
    assert {
        "date_str": "2017-09-24",
        "datetime_str": "2017-09-24T15:36:09",
        "key": "sample key",
    } == TestDateController.get_response_model.fields_example
    assert {
        "date_str": None,
        "datetime_str": None,
        "key": None,
    } == TestDateController.get_response_model.fields_default
    assert {
        "date_str": False,
        "datetime_str": False,
        "key": False,
    } == TestDateController.get_response_model.fields_required
    assert {
        "date_str": False,
        "datetime_str": False,
        "key": False,
    } == TestDateController.get_response_model.fields_readonly
