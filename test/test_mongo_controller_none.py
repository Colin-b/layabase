import pytest

import layabase
import layabase.database_mongo


@pytest.fixture
def controller_insert():
    class TestCollectionInsert:
        __collection_name__ = "test"

        key = layabase.database_mongo.Column(int, is_primary_key=True)
        my_dict = layabase.database_mongo.DictColumn(
            fields={"null_value": layabase.database_mongo.Column(store_none=True)},
            is_required=True,
        )

    return layabase.CRUDController(TestCollectionInsert, skip_name_check=True)


@pytest.fixture
def controller_not_inserted():
    class TestCollectionNotInserted:
        __collection_name__ = "test"

        key = layabase.database_mongo.Column(int, is_primary_key=True)
        my_dict = layabase.database_mongo.DictColumn(
            fields={"null_value": layabase.database_mongo.Column(store_none=False)},
            is_required=True,
        )

    return layabase.CRUDController(TestCollectionNotInserted)


@pytest.fixture
def controller_retrieve():
    class TestCollectionRetrieve:
        __collection_name__ = "test"

        key = layabase.database_mongo.Column(int, is_primary_key=True)
        my_dict = layabase.database_mongo.Column(dict, is_required=True)

    return layabase.CRUDController(TestCollectionRetrieve, skip_name_check=True)


@pytest.fixture
def controllers(controller_insert, controller_not_inserted, controller_retrieve):
    return layabase.load(
        "mongomock", [controller_insert, controller_not_inserted, controller_retrieve]
    )


def test_get_retrieve_none_field_when_not_in_collection(
    controllers, controller_insert, controller_retrieve
):
    controller_insert.post({"key": 1, "my_dict": {"null_value": None}})
    assert controller_retrieve.get({}) == [{"key": 1, "my_dict": {"null_value": None}}]


def test_get_do_not_retrieve_none_field_when_not_in_collection(
    controllers, controller_not_inserted, controller_retrieve
):
    controller_not_inserted.post({"key": 1, "my_dict": {"null_value": None}})
    assert controller_retrieve.get({}) == [{"key": 1, "my_dict": {}}]
