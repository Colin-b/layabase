import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        prim_def_inc = layabase.mongo.Column(
            int, is_primary_key=True, default_value=1, should_auto_increment=True
        )
        prim_def = layabase.mongo.Column(int, is_primary_key=True, default_value=1)
        prim_inc = layabase.mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_put_without_primary_and_incremented_field(controller):
    controller.post({"prim_def": 1})
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({"prim_def": 1})
    assert exception_info.value.errors == {
        "prim_inc": ["Missing data for required field."]
    }
    assert exception_info.value.received_data == {"prim_def": 1}
