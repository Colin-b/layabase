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
                "first_key": layabase.mongo.Column(EnumTest, is_nullable=False),
                "second_key": layabase.mongo.Column(int, is_nullable=False),
            },
            is_required=True,
            is_nullable=False,
        )

    controller = layabase.CRUDController(TestCollection, history=True)
    layabase.load("mongomock", [controller])
    return controller


def test_post_without_providing_required_non_nullable_dict_column_is_invalid(
    controller,
):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"key": "first"})
    assert {
        "dict_field": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "first"} == exception_info.value.received_data


def test_put_without_providing_required_non_nullable_dict_column_is_valid(controller):
    controller.post(
        {"key": "first", "dict_field": {"first_key": "Value1", "second_key": 0}}
    )
    assert (
        {
            "dict_field": {"first_key": "Value1", "second_key": 0},
            "key": "first",
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 0},
            "key": "first",
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
    ) == controller.put({"key": "first"})


def test_put_with_null_provided_required_non_nullable_dict_column_is_invalid(
    controller,
):
    controller.post(
        {"key": "first", "dict_field": {"first_key": "Value1", "second_key": 0}}
    )
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({"key": "first", "dict_field": None})
    assert {
        "dict_field": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "first", "dict_field": None} == exception_info.value.received_data
