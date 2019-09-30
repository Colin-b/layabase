import enum

import pytest

import layabase
import layabase.database_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller():
    class TestController(layabase.CRUDController):
        class TestOptionalDictModel:
            __tablename__ = "optional_dict_table_name"

            key = layabase.database_mongo.Column(str, is_primary_key=True)
            dict_col = layabase.database_mongo.DictColumn(
                get_fields=lambda model_as_dict: {
                    "first_key": layabase.database_mongo.Column(
                        EnumTest, is_nullable=True
                    ),
                    "second_key": layabase.database_mongo.Column(int, is_nullable=True),
                }
            )

        model = TestOptionalDictModel

    _db = layabase.load("mongomock", [TestController])
    yield TestController
    layabase.testing.reset(_db)


def test_post_missing_optional_dict_is_valid(controller):
    assert {
        "dict_col": {"first_key": None, "second_key": None},
        "key": "my_key",
    } == controller.post({"key": "my_key"})


def test_post_optional_dict_as_none_is_valid(controller):
    assert {
        "dict_col": {"first_key": None, "second_key": None},
        "key": "my_key",
    } == controller.post({"key": "my_key", "dict_col": None})


def test_put_missing_optional_dict_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
    ) == controller.put({"key": "my_key"})


def test_post_empty_optional_dict_is_valid(controller):
    assert {"key": "my_key", "dict_col": {}} == controller.post(
        {"key": "my_key", "dict_col": {}}
    )


def test_put_empty_optional_dict_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
        {"key": "my_key", "dict_col": {}},
    ) == controller.put({"key": "my_key", "dict_col": {}})


def test_put_optional_dict_as_none_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
    ) == controller.put({"key": "my_key", "dict_col": None})


def test_get_optional_dict_as_none_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert [
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    ] == controller.get({"dict_col": None})


def test_delete_optional_dict_as_none_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == controller.delete({"dict_col": None})
