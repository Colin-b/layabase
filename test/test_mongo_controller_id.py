import pytest

from layaberr import ValidationFailed

from layabase import database, database_mongo
import layabase.testing


class TestIdController(database.CRUDController):
    pass


def _create_models(base):
    class TestIdModel(database_mongo.CRUDModel, base=base, table_name="id_table_name"):
        _id = database_mongo.Column(is_primary_key=True)

    TestIdController.model(TestIdModel)

    return [TestIdModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    yield _db
    layabase.testing.reset(_db)


def test_post_id_is_valid(db):
    assert {"_id": "123456789abcdef012345678"} == TestIdController.post(
        {"_id": "123456789ABCDEF012345678"}
    )


def test_invalid_id_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestIdController.post({"_id": "invalid value"})
    assert {
        "_id": [
            "'invalid value' is not a valid ObjectId, it must be a 12-byte input or a 24-character hex string"
        ]
    } == exception_info.value.errors
    assert {"_id": "invalid value"} == exception_info.value.received_data
