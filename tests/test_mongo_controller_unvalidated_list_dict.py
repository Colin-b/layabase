import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        float_key = layabase.mongo.Column(float, is_primary_key=True)
        float_with_default = layabase.mongo.Column(float, default_value=34)
        dict_field = layabase.mongo.Column(dict, is_required=True)
        list_field = layabase.mongo.Column(list, is_required=True)

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_post_float_as_int(controller):
    assert controller.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    ) == {
        "dict_field": {"any_key": 5},
        "float_key": 1,
        "float_with_default": 34,
        "list_field": [22, "33", 44.55, True],
    }


def test_get_float_as_int(controller):
    controller.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    )
    assert controller.get_one({"float_key": 1}) == {
        "dict_field": {"any_key": 5},
        "float_key": 1,
        "float_with_default": 34,
        "list_field": [22, "33", 44.55, True],
    }


def test_put_float_as_int(controller):
    controller.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    )
    assert controller.put(
        {"dict_field.any_key": 6, "float_key": 1, "float_with_default": 35}
    ) == (
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "float_with_default": 34,
            "list_field": [22, "33", 44.55, True],
        },
        {
            "dict_field": {"any_key": 6},
            "float_key": 1,
            "float_with_default": 35,
            "list_field": [22, "33", 44.55, True],
        },
    )


def test_get_with_required_field_as_none_is_invalid(controller):
    controller.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    )
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.get({"dict_field": None})
    assert exception_info.value.errors == {
        "dict_field": ["Missing data for required field."]
    }
    assert exception_info.value.received_data == {"dict_field": None}
