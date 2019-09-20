import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo
from test.flask_restplus_mock import TestAPI


class TestNullableAutoSetController(database.CRUDController):
    pass


def _create_models(base):
    class TestNullableAutoSetModel(
        database_mongo.CRUDModel, base=base, table_name="nullable_auto_set_table_name"
    ):
        prim_def_inc = database_mongo.Column(
            int, is_primary_key=True, default_value=1, should_auto_increment=True
        )
        prim_def = database_mongo.Column(int, is_primary_key=True, default_value=1)
        prim_inc = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )

    TestNullableAutoSetController.model(TestNullableAutoSetModel)

    return [TestNullableAutoSetModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    TestNullableAutoSetController.namespace(TestAPI)

    yield _db

    database.reset(_db)


def test_put_without_primary_and_incremented_field(db):
    TestNullableAutoSetController.post({"prim_def": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        TestNullableAutoSetController.put({"prim_def": 1})
    assert {
        "prim_inc": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"prim_def": 1} == exception_info.value.received_data
