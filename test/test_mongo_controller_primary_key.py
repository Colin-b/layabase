import pytest
import pymongo.errors

import layabase
import layabase.mongo


def test_2entities_on_same_collection_without_pk():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        mandatory = layabase.mongo.Column(int, is_nullable=False)
        optional = layabase.mongo.Column(str)

    controller = layabase.CRUDController(TestCollection, history=True)

    mongo_base = layabase.load("mongomock", [controller])
    controller.post({"key": "1", "mandatory": 2})
    controller.post({"key": "2", "mandatory": 2})

    class TestCollection2:
        __collection_name__ = "test"

    controller = layabase.CRUDController(TestCollection2, history=True)

    with pytest.raises(pymongo.errors.DuplicateKeyError):
        layabase.mongo.link(controller, mongo_base)


def test_2entities_on_same_collection_with_pk():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        mandatory = layabase.mongo.Column(int, is_nullable=False)
        optional = layabase.mongo.Column(str)

    controller = layabase.CRUDController(TestCollection, history=True)

    mongo_base = layabase.load("mongomock", [controller])
    controller.post({"key": "1", "mandatory": 2})
    controller.post({"key": "2", "mandatory": 2})

    class TestCollection2:
        __collection_name__ = "test"

    controller = layabase.CRUDController(
        TestCollection2, history=True, skip_update_indexes=True
    )

    layabase.mongo.link(controller, mongo_base)

    controller.post({"key": "3", "mandatory": 2})
    assert len(controller.get({})) == 3
