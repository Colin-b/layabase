import pytest

from layaberr import ValidationFailed

import layabase
import layabase.database_mongo
import layabase.testing


@pytest.fixture
def controller():
    class TestController(layabase.CRUDController):
        class TestIdModel:
            __tablename__ = "id_table_name"

            _id = layabase.database_mongo.Column(is_primary_key=True)

        model = TestIdModel

    _db = layabase.load("mongomock", [TestController])
    yield TestController
    layabase.testing.reset(_db)


def test_post_id_is_valid(controller):
    assert controller.post({"_id": "123456789ABCDEF012345678"}) == {
        "_id": "123456789abcdef012345678"
    }


def test_invalid_id_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"_id": "invalid value"})
    assert exception_info.value.errors == {
        "_id": [
            "'invalid value' is not a valid ObjectId, it must be a 12-byte input or a 24-character hex string"
        ]
    }
    assert exception_info.value.received_data == {"_id": "invalid value"}
