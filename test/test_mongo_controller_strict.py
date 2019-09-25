import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo
import layabase.testing


class TestStrictController(database.CRUDController):
    pass


def _create_models(base):
    class TestStrictModel(
        database_mongo.CRUDModel,
        base=base,
        table_name="strict_table_name",
        skip_unknown_fields=False,
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        mandatory = database_mongo.Column(int, is_nullable=False)
        optional = database_mongo.Column(str)

    TestStrictController.model(TestStrictModel)

    return [TestStrictModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    yield _db
    layabase.testing.reset(_db)


def test_post_with_unknown_field_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestStrictController.post(
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            }
        )
    assert {"unknown": ["Unknown field"]} == exception_info.value.errors
    assert {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
        "unknown": "my_value",
    } == exception_info.value.received_data
