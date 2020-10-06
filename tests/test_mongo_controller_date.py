import datetime

import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        date_str = layabase.mongo.Column(datetime.date)
        datetime_str = layabase.mongo.Column(datetime.datetime)

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_put_is_updating_date(controller: layabase.CRUDController):
    controller.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert controller.put(
        {
            "key": "my_key1",
            "date_str": "2018-06-01",
            "datetime_str": "1989-12-31T01:00:00",
        }
    ) == (
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
    )
    assert controller.get({"date_str": "2018-06-01"}) == [
        {
            "date_str": "2018-06-01",
            "datetime_str": "1989-12-31T01:00:00",
            "key": "my_key1",
        }
    ]


def test_get_date_is_handled_for_valid_date(controller: layabase.CRUDController):
    controller.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    d = datetime.datetime.strptime("2017-05-15", "%Y-%m-%d").date()
    assert controller.get({"date_str": d}) == [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ]


def test_post_invalid_date_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post(
            {
                "key": "my_key1",
                "date_str": "this is not a date",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
    assert exception_info.value.errors == {"date_str": ["Not a valid date."]}
    assert exception_info.value.received_data == {
        "key": "my_key1",
        "date_str": "this is not a date",
        "datetime_str": "2016-09-23T23:59:59",
    }


def test_get_invalid_date_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.get({"date_str": "this is not a date"})
    assert exception_info.value.errors == {"date_str": ["Not a valid date."]}
    assert exception_info.value.received_data == {"date_str": "this is not a date"}


def test_delete_invalid_date_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.delete({"date_str": "this is not a date"})
    assert exception_info.value.errors == {"date_str": ["Not a valid date."]}
    assert exception_info.value.received_data == {"date_str": "this is not a date"}


def test_get_with_unknown_fields_is_valid(controller: layabase.CRUDController):
    controller.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert controller.get({"date_str": "2018-12-30", "unknown_field": "value"}) == [
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    ]


def test_put_with_unknown_fields_is_valid(controller: layabase.CRUDController):
    controller.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert controller.put(
        {"key": "my_key1", "date_str": "2018-12-31", "unknown_field": "value"}
    ) == (
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
    )
    assert controller.get({"date_str": "2018-12-31"}) == [
        {
            "key": "my_key1",
            "date_str": "2018-12-31",
            "datetime_str": "2016-09-23T23:59:59",
        }
    ]
    assert controller.get({"date_str": "2018-12-30"}) == []


def test_put_unexisting_is_invalid(controller: layabase.CRUDController):
    controller.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({"key": "my_key2"})
    assert exception_info.value.requested_data == {"key": "my_key2"}


def test_post_invalid_datetime_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post(
            {
                "key": "my_key1",
                "date_str": "2016-09-23",
                "datetime_str": "This is not a valid datetime",
            }
        )
    assert exception_info.value.errors == {"datetime_str": ["Not a valid datetime."]}
    assert exception_info.value.received_data == {
        "key": "my_key1",
        "date_str": "2016-09-23",
        "datetime_str": "This is not a valid datetime",
    }


def test_post_datetime_for_a_date_is_valid(controller: layabase.CRUDController):
    assert controller.post(
        {
            "key": "my_key1",
            "date_str": datetime.datetime.strptime(
                "2017-05-01T01:05:45", "%Y-%m-%dT%H:%M:%S"
            ),
            "datetime_str": "2017-05-30T01:05:45",
        }
    ) == {
        "key": "my_key1",
        "date_str": "2017-05-01",
        "datetime_str": "2017-05-30T01:05:45+00:00",
    }


def test_get_date_is_handled_for_unused_date(controller: layabase.CRUDController):
    controller.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    d = datetime.datetime.strptime("2016-09-23", "%Y-%m-%d").date()
    assert controller.get({"date_str": d}) == []


def test_get_date_is_handled_for_valid_datetime(controller: layabase.CRUDController):
    controller.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    dt = datetime.datetime.strptime("2016-09-23T23:59:59", "%Y-%m-%dT%H:%M:%S")
    assert controller.get({"datetime_str": dt}) == [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ]


def test_get_date_is_handled_for_unused_datetime(controller: layabase.CRUDController):
    controller.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    dt = datetime.datetime.strptime("2016-09-24T23:59:59", "%Y-%m-%dT%H:%M:%S")
    assert controller.get({"datetime_str": dt}) == []
