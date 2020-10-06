import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        int_choices_field = layabase.mongo.Column(
            int, description="Test Documentation", choices=[1, 2, 3]
        )
        str_choices_field = layabase.mongo.Column(
            str, description="Test Documentation", choices=["one", "two", "three"]
        )
        float_choices_field = layabase.mongo.Column(
            float, description="Test Documentation", choices=[1.25, 1.5, 1.75]
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_post_with_choices_field_with_a_value_not_in_choices_list_is_invalid(
    controller: layabase.CRUDController
):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post(
            {
                "key": 1,
                "int_choices_field": 4,
                "str_choices_field": "four",
                "float_choices_field": 2.5,
            }
        )
    assert exception_info.value.errors == {
        "float_choices_field": ['Value "2.5" is not within [1.25, 1.5, 1.75].'],
        "int_choices_field": ['Value "4" is not within [1, 2, 3].'],
        "str_choices_field": ["Value \"four\" is not within ['one', 'two', 'three']."],
    }
    assert exception_info.value.received_data == {
        "int_choices_field": 4,
        "str_choices_field": "four",
        "float_choices_field": 2.5,
    }
