import enum

import pytest

import layabase
import layabase.mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        enum_field = layabase.mongo.Column(
            EnumTest, is_nullable=False, description="Test Documentation"
        )
        optional_with_default = layabase.mongo.Column(str, default_value="Test value")

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_post_with_specified_incremented_field_is_ignored_and_valid(controller: layabase.CRUDController):
    assert controller.post({"key": 0, "enum_field": "Value1"}) == {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    }


def test_post_with_enum_is_valid(controller: layabase.CRUDController):
    assert controller.post({"key": 0, "enum_field": EnumTest.Value1}) == {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    }


def test_post_with_invalid_enum_choice_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"key": 0, "enum_field": "InvalidValue"})
    assert exception_info.value.errors == {
        "enum_field": ["Value \"InvalidValue\" is not within ['Value1', 'Value2']."]
    }
    assert exception_info.value.received_data == {"enum_field": "InvalidValue", "key": 0}


def test_post_many_with_specified_incremented_field_is_ignored_and_valid(
    controller: layabase.CRUDController
):
    assert controller.post_many(
        [
            {"key": 0, "enum_field": "Value1"},
            {"key": 0, "enum_field": "Value2"},
        ]
    ) == [
        {"optional_with_default": "Test value", "enum_field": "Value1", "key": 1},
        {"optional_with_default": "Test value", "enum_field": "Value2", "key": 2},
    ]
