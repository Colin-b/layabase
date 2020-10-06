import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)

    controller = layabase.CRUDController(TestCollection, skip_unknown_fields=False)
    layabase.load("mongomock", [controller])
    return controller


def test_unknown_field_failure_put_many(controller: layabase.CRUDController):
    controller.post({"key": "my_key1"})
    controller.post({"key": "my_key2"})
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put_many([{"key": "my_key1", "unknown": "my_value1"}])
    assert exception_info.value.errors == {0: {"unknown": ["Unknown field"]}}
    assert exception_info.value.received_data == [
        {"key": "my_key1", "unknown": "my_value1"}
    ]


def test_put_many_unexisting(controller: layabase.CRUDController):
    controller.post({"key": "my_key1"})
    controller.post({"key": "my_key2"})
    with pytest.raises(layabase.ModelCouldNotBeFound) as exception_info:
        controller.put_many([{"key": "my_key3"}])
    assert exception_info.value.requested_data == {"key": "my_key3"}
