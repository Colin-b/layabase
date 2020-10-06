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

        id = layabase.mongo.Column(int, is_primary_key=True)
        enum_field = layabase.mongo.Column(
            EnumTest, allow_none_as_filter=True, store_none=True
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_post_none_enum_value_is_valid(controller):
    assert controller.post({"id": 1, "enum_field": None}) == {
        "enum_field": None,
        "id": 1,
    }


def test_post_not_provided_enum_value_is_valid(controller):
    assert controller.post({"id": 1}) == {"enum_field": None, "id": 1}


def test_get_none_enum_value_is_valid(controller):
    controller.post({"id": 1, "enum_field": None})
    assert controller.get({"enum_field": None}) == [{"enum_field": None, "id": 1}]


def test_get_not_provided_enum_value_is_valid(controller):
    controller.post({"id": 1, "enum_field": None})
    assert controller.get({}) == [{"enum_field": None, "id": 1}]
