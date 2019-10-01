import pytest
from layaberr import ValidationFailed

import layabase
import layabase.database_mongo


@pytest.fixture
def controller():
    class TestModel:
        __tablename__ = "test"

        prim_def_inc = layabase.database_mongo.Column(
            int, is_primary_key=True, default_value=1, should_auto_increment=True
        )
        prim_def = layabase.database_mongo.Column(
            int, is_primary_key=True, default_value=1
        )
        prim_inc = layabase.database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )

    controller = layabase.CRUDController(TestModel)
    layabase.load("mongomock", [controller])
    return controller


def test_put_without_primary_and_incremented_field(controller):
    controller.post({"prim_def": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put({"prim_def": 1})
    assert exception_info.value.errors == {
        "prim_inc": ["Missing data for required field."]
    }
    assert exception_info.value.received_data == {"prim_def": 1}
