import datetime
import re

import pytest
from layaberr import ValidationFailed

import layabase
import layabase._database_mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        unique_key = layabase._database_mongo.Column(str, is_primary_key=True)
        non_unique_key = layabase._database_mongo.Column(
            datetime.date, index_type=layabase._database_mongo.IndexType.Other
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_post_twice_with_unique_index_is_invalid(controller):
    assert {"non_unique_key": "2017-01-01", "unique_key": "test"} == controller.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"unique_key": "test", "non_unique_key": "2017-01-02"})
    assert {"": ["This document already exists."]} == exception_info.value.errors
    assert {
        "non_unique_key": "2017-01-02",
        "unique_key": "test",
    } == exception_info.value.received_data


def test_get_all_without_primary_key_is_valid(controller):
    assert {"non_unique_key": "2017-01-01", "unique_key": "test"} == controller.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    assert [{"non_unique_key": "2017-01-01", "unique_key": "test"}] == controller.get(
        {}
    )


def test_get_one_and_multiple_results_is_invalid(controller):
    controller.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    controller.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    with pytest.raises(ValidationFailed) as exception_info:
        controller.get_one({})
    assert {
        "": ["More than one result: Consider another filtering."]
    } == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_get_one_is_valid(controller):
    controller.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    controller.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert controller.get_one({"unique_key": "test2"}) == {
        "unique_key": "test2",
        "non_unique_key": "2017-01-01",
    }


def test_get_with_list_is_valid(controller):
    controller.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    controller.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert controller.get({"unique_key": ["test", "test2"]}) == [
        {"unique_key": "test", "non_unique_key": "2017-01-01"},
        {"unique_key": "test2", "non_unique_key": "2017-01-01"},
    ]


def test_get_with_partial_matching_list_is_valid(controller):
    controller.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    controller.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert controller.get({"unique_key": ["test2"]}) == [
        {"unique_key": "test2", "non_unique_key": "2017-01-01"}
    ]


def test_get_with_empty_list_is_valid(controller):
    controller.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    controller.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert controller.get({"unique_key": []}) == [
        {"unique_key": "test", "non_unique_key": "2017-01-01"},
        {"unique_key": "test2", "non_unique_key": "2017-01-01"},
    ]


def test_get_with_partialy_matching_and_not_matching_list_is_valid(controller):
    controller.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    controller.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert [{"unique_key": "test", "non_unique_key": "2017-01-01"}] == controller.get(
        {"unique_key": ["not existing", "test", "another non existing"]}
    )


def test_delete_with_list_is_valid(controller):
    controller.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    controller.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 2 == controller.delete({"unique_key": ["test", "test2"]})


def test_delete_with_partial_matching_list_is_valid(controller):
    controller.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    controller.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 1 == controller.delete({"unique_key": ["test2"]})


def test_non_iso8601_date_failure(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"unique_key": "test", "non_unique_key": "12/06/2017"})
    assert {"non_unique_key": ["Not a valid date."]} == exception_info.value.errors
    assert {
        "unique_key": "test",
        "non_unique_key": "12/06/2017",
    } == exception_info.value.received_data


def test_delete_with_empty_list_is_valid(controller):
    controller.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    controller.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 2 == controller.delete({"unique_key": []})


def test_delete_with_partialy_matching_and_not_matching_list_is_valid(controller):
    controller.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    controller.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 1 == controller.delete(
        {"unique_key": ["not existing", "test", "another non existing"]}
    )


def test_get_one_without_result_is_valid(controller):
    controller.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    controller.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert {} == controller.get_one({"unique_key": "test3"})


def test_get_field_names_valid(controller):
    assert ["non_unique_key", "unique_key"] == controller.get_field_names()


def _assert_regex(expected, actual):
    assert re.match(
        f"{expected}".replace("[", "\\[").replace("]", "\\]").replace("\\\\", "\\"),
        f"{actual}",
    )


def test_get_all_with_none_primary_key_is_valid(controller):
    assert {"non_unique_key": "2017-01-01", "unique_key": "test"} == controller.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    assert [{"non_unique_key": "2017-01-01", "unique_key": "test"}] == controller.get(
        {"unique_key": None}
    )


def test_post_many_with_same_unique_index_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many(
            [
                {"unique_key": "test", "non_unique_key": "2017-01-01"},
                {"unique_key": "test", "non_unique_key": "2017-01-01"},
            ]
        )
    assert re.match(
        "{'writeErrors': [{'index': 1, 'code': 11000, 'errmsg': 'E11000 Duplicate Key Error', 'op': {'unique_key': 'test', 'non_unique_key': "
        "datetime.datetime(2017, 1, 1, 0, 0), '_id': ObjectId('.*')}}], 'nInserted': 1}".replace(
            "[", "\["
        )
        .replace("]", "\]")
        .replace("(", "\(")
        .replace(")", "\)"),
        str(exception_info.value.errors[""][0]),
    )
    assert [
        {"unique_key": "test", "non_unique_key": "2017-01-01"},
        {"unique_key": "test", "non_unique_key": "2017-01-01"},
    ] == exception_info.value.received_data


def test_post_different_unique_index_is_valid(controller):
    assert {"non_unique_key": "2017-01-01", "unique_key": "test"} == controller.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    assert {"non_unique_key": "2017-01-01", "unique_key": "test2"} == controller.post(
        {"unique_key": "test2", "non_unique_key": "2017-01-01"}
    )
    assert [
        {"non_unique_key": "2017-01-01", "unique_key": "test"},
        {"non_unique_key": "2017-01-01", "unique_key": "test2"},
    ] == controller.get({})
