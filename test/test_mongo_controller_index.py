import datetime
import re

import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo


class TestIndexController(database.CRUDController):
    pass


def _create_models(base):
    class TestIndexModel(
        database_mongo.CRUDModel, base=base, table_name="index_table_name"
    ):
        unique_key = database_mongo.Column(str, is_primary_key=True)
        non_unique_key = database_mongo.Column(
            datetime.date, index_type=database_mongo.IndexType.Other
        )

    TestIndexController.model(TestIndexModel)

    return [TestIndexModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    yield _db
    database.reset(_db)


def test_post_twice_with_unique_index_is_invalid(db):
    assert {
        "non_unique_key": "2017-01-01",
        "unique_key": "test",
    } == TestIndexController.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    with pytest.raises(ValidationFailed) as exception_info:
        TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-02"})
    assert {"": ["This document already exists."]} == exception_info.value.errors
    assert {
        "non_unique_key": "2017-01-02",
        "unique_key": "test",
    } == exception_info.value.received_data


def test_get_all_without_primary_key_is_valid(db):
    assert {
        "non_unique_key": "2017-01-01",
        "unique_key": "test",
    } == TestIndexController.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    assert [
        {"non_unique_key": "2017-01-01", "unique_key": "test"}
    ] == TestIndexController.get({})


def test_get_one_and_multiple_results_is_invalid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    with pytest.raises(ValidationFailed) as exception_info:
        TestIndexController.get_one({})
    assert {
        "": ["More than one result: Consider another filtering."]
    } == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_get_one_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert {
        "unique_key": "test2",
        "non_unique_key": "2017-01-01",
    } == TestIndexController.get_one({"unique_key": "test2"})


def test_get_with_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert TestIndexController.get({"unique_key": ["test", "test2"]}) == [
        {"unique_key": "test", "non_unique_key": "2017-01-01"},
        {"unique_key": "test2", "non_unique_key": "2017-01-01"},
    ]


def test_get_with_partial_matching_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert TestIndexController.get({"unique_key": ["test2"]}) == [
        {"unique_key": "test2", "non_unique_key": "2017-01-01"}
    ]


def test_get_with_empty_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert TestIndexController.get({"unique_key": []}) == [
        {"unique_key": "test", "non_unique_key": "2017-01-01"},
        {"unique_key": "test2", "non_unique_key": "2017-01-01"},
    ]


def test_get_with_partialy_matching_and_not_matching_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert [
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    ] == TestIndexController.get(
        {"unique_key": ["not existing", "test", "another non existing"]}
    )


def test_delete_with_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 2 == TestIndexController.delete({"unique_key": ["test", "test2"]})


def test_delete_with_partial_matching_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 1 == TestIndexController.delete({"unique_key": ["test2"]})


def test_non_iso8601_date_failure(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestIndexController.post({"unique_key": "test", "non_unique_key": "12/06/2017"})
    assert {"non_unique_key": ["Not a valid date."]} == exception_info.value.errors
    assert {
        "unique_key": "test",
        "non_unique_key": "12/06/2017",
    } == exception_info.value.received_data


def test_delete_with_empty_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 2 == TestIndexController.delete({"unique_key": []})


def test_delete_with_partialy_matching_and_not_matching_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 1 == TestIndexController.delete(
        {"unique_key": ["not existing", "test", "another non existing"]}
    )


def test_get_one_without_result_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert {} == TestIndexController.get_one({"unique_key": "test3"})


def test_get_field_names_valid(db):
    assert ["non_unique_key", "unique_key"] == TestIndexController.get_field_names()


def _assert_regex(expected, actual):
    assert re.match(
        f"{expected}".replace("[", "\\[").replace("]", "\\]").replace("\\\\", "\\"),
        f"{actual}",
    )


def test_get_all_with_none_primary_key_is_valid(db):
    assert {
        "non_unique_key": "2017-01-01",
        "unique_key": "test",
    } == TestIndexController.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    assert [
        {"non_unique_key": "2017-01-01", "unique_key": "test"}
    ] == TestIndexController.get({"unique_key": None})


def test_post_many_with_same_unique_index_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestIndexController.post_many(
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


def test_post_different_unique_index_is_valid(db):
    assert {
        "non_unique_key": "2017-01-01",
        "unique_key": "test",
    } == TestIndexController.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    assert {
        "non_unique_key": "2017-01-01",
        "unique_key": "test2",
    } == TestIndexController.post(
        {"unique_key": "test2", "non_unique_key": "2017-01-01"}
    )
    assert [
        {"non_unique_key": "2017-01-01", "unique_key": "test"},
        {"non_unique_key": "2017-01-01", "unique_key": "test2"},
    ] == TestIndexController.get({})
