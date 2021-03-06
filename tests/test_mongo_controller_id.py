import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        _id = layabase.mongo.Column(is_primary_key=True)

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_post_id_is_valid(controller):
    assert controller.post({"_id": "123456789ABCDEF012345678"}) == {
        "_id": "123456789abcdef012345678"
    }


def test_invalid_id_is_invalid(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"_id": "invalid value"})
    assert exception_info.value.errors == {
        "_id": [
            "'invalid value' is not a valid ObjectId, it must be a 12-byte input or a 24-character hex string"
        ]
    }
    assert exception_info.value.received_data == {"_id": "invalid value"}
