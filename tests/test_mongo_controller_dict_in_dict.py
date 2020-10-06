import enum

import pytest

import layabase
import layabase.mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(is_primary_key=True)
        dict_field = layabase.mongo.DictColumn(
            fields={
                "first_key": layabase.mongo.DictColumn(
                    fields={
                        "inner_key1": layabase.mongo.Column(
                            EnumTest, is_nullable=False
                        ),
                        "inner_key2": layabase.mongo.Column(int, is_nullable=False),
                    },
                    is_required=True,
                ),
                "second_key": layabase.mongo.Column(int, is_nullable=False),
            },
            is_required=True,
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_get_with_dot_notation_multi_level_is_valid(controller):
    assert {
        "dict_field": {
            "first_key": {"inner_key1": "Value1", "inner_key2": 3},
            "second_key": 3,
        },
        "key": "my_key",
    } == controller.post(
        {
            "key": "my_key",
            "dict_field": {
                "first_key": {"inner_key1": EnumTest.Value1, "inner_key2": 3},
                "second_key": 3,
            },
        }
    )
    assert {
        "dict_field": {
            "first_key": {"inner_key1": "Value2", "inner_key2": 3},
            "second_key": 3,
        },
        "key": "my_key2",
    } == controller.post(
        {
            "key": "my_key2",
            "dict_field": {
                "first_key": {"inner_key1": EnumTest.Value2, "inner_key2": 3},
                "second_key": 3,
            },
        }
    )
    assert [
        {
            "dict_field": {
                "first_key": {"inner_key1": "Value1", "inner_key2": 3},
                "second_key": 3,
            },
            "key": "my_key",
        }
    ] == controller.get({"dict_field.first_key.inner_key1": EnumTest.Value1})


def test_get_and_delete_parsers_contains_multi_level_dot_notation_fields(controller):
    assert {
        "dict_field.first_key.inner_key1",
        "dict_field.first_key.inner_key2",
    }.issubset({arg.name for arg in controller.query_delete_parser.args})

    assert {
        "dict_field.first_key.inner_key1",
        "dict_field.first_key.inner_key2",
    }.issubset({arg.name for arg in controller.query_get_parser.args})
