import enum

import pytest

import layabase
import layabase._database_mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase._database_mongo.Column(str, is_primary_key=True)
        dict_col = layabase._database_mongo.DictColumn(
            get_fields=lambda document: {
                "first_key": layabase._database_mongo.Column(
                    EnumTest, is_nullable=True
                ),
                "second_key": layabase._database_mongo.Column(int, is_nullable=True),
            }
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_post_missing_optional_dict_is_valid(controller):
    assert controller.post({"key": "my_key"}) == {
        "dict_col": {"first_key": None, "second_key": None},
        "key": "my_key",
    }


def test_post_optional_dict_as_none_is_valid(controller):
    assert controller.post({"key": "my_key", "dict_col": None}) == {
        "dict_col": {"first_key": None, "second_key": None},
        "key": "my_key",
    }


def test_put_missing_optional_dict_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert controller.put({"key": "my_key"}) == (
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
    )


def test_post_empty_optional_dict_is_valid(controller):
    assert controller.post({"key": "my_key", "dict_col": {}}) == {
        "key": "my_key",
        "dict_col": {},
    }


def test_put_empty_optional_dict_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert controller.put({"key": "my_key", "dict_col": {}}) == (
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
        {"key": "my_key", "dict_col": {}},
    )


def test_put_optional_dict_as_none_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert controller.put({"key": "my_key", "dict_col": None}) == (
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
    )


def test_get_optional_dict_as_none_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert controller.get({"dict_col": None}) == [
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    ]


def test_delete_optional_dict_as_none_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert controller.delete({"dict_col": None}) == 1
