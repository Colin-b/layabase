import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        mandatory = layabase.mongo.Column(int, is_nullable=False)
        optional = layabase.mongo.Column(str)

    controller = layabase.CRUDController(TestCollection, skip_unknown_fields=False)
    layabase.load("mongomock", [controller])
    return controller


def test_post_with_unknown_field_is_invalid(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post(
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            }
        )
    assert exception_info.value.errors == {"unknown": ["Unknown field"]}
    assert exception_info.value.received_data == {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
        "unknown": "my_value",
    }
