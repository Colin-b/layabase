import enum

import mongomock
import pytest
import pymongo.errors

import layabase
import layabase.mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


def test_post_versioning_is_valid(monkeypatch):
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(is_primary_key=True)
        dict_field = layabase.mongo.DictColumn(
            fields={
                "first_key": layabase.mongo.Column(EnumTest, is_nullable=False),
                "second_key": layabase.mongo.Column(int, is_nullable=False),
            },
            is_required=True,
        )

    controller = layabase.CRUDController(TestCollection, history=True)
    monkeypatch.setattr(
        mongomock.MongoClient, "server_info", lambda *args: {"version": "3.2.0"}
    )
    layabase.load("mongomock", [controller])
    # Reset this fake value
    layabase._database_mongo._server_versions = {}

    assert controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    ) == {
        "key": "first",
        "dict_field": {"first_key": "Value1", "second_key": 1},
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }
    assert controller.get_history({}) == [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        }
    ]
    assert controller.get({}) == [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        }
    ]


def test_index_creation_failure_with_new_index_duplicate(monkeypatch):
    class TestCollection:
        __collection_name__ = "test"

        id = layabase.mongo.Column()

    controller = layabase.CRUDController(TestCollection)
    monkeypatch.setattr(
        mongomock.MongoClient, "server_info", lambda *args: {"version": "3.2.0"}
    )
    base = layabase.load("mongomock", [controller])

    controller.post_many([{"id": "1"}, {"id": "1"}])

    class TestCollection2:
        __collection_name__ = "test"

        id = layabase.mongo.Column(index_type=layabase.mongo.IndexType.Unique)

    controller2 = layabase.CRUDController(TestCollection2, history=True)
    monkeypatch.setattr(
        mongomock.MongoClient, "server_info", lambda *args: {"version": "3.2.0"}
    )
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        layabase.mongo.link(controller2, base)
    # Reset this fake value
    layabase._database_mongo._server_versions = {}
