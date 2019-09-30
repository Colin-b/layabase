import pytest

from layabase import database, database_mongo
import layabase.testing


class TestNoneInsertController(database.CRUDController):
    class TestNoneInsertModel:
        __tablename__ = "none_table_name"

        key = database_mongo.Column(int, is_primary_key=True)
        my_dict = database_mongo.DictColumn(
            fields={"null_value": database_mongo.Column(store_none=True)},
            is_required=True,
        )

    model = TestNoneInsertModel
    skip_name_check = True


class TestNoneRetrieveController(database.CRUDController):
    class TestNoneRetrieveModel:
        __tablename__ = "none_table_name"

        key = database_mongo.Column(int, is_primary_key=True)
        my_dict = database_mongo.Column(dict, is_required=True)

    model = TestNoneRetrieveModel
    skip_name_check = True


class TestNoneNotInsertedController(database.CRUDController):
    class TestNoneNotInsertedModel:
        __tablename__ = "none_table_name"

        key = database_mongo.Column(int, is_primary_key=True)
        my_dict = database_mongo.DictColumn(
            fields={"null_value": database_mongo.Column(store_none=False)},
            is_required=True,
        )

    model = TestNoneNotInsertedModel


@pytest.fixture
def db():
    _db = database.load(
        "mongomock",
        [
            TestNoneInsertController,
            TestNoneNotInsertedController,
            TestNoneRetrieveController,
        ],
    )
    yield _db
    layabase.testing.reset(_db)


def test_get_retrieve_none_field_when_not_in_model(db):
    TestNoneInsertController.post({"key": 1, "my_dict": {"null_value": None}})
    assert [
        {"key": 1, "my_dict": {"null_value": None}}
    ] == TestNoneRetrieveController.get({})


def test_get_do_not_retrieve_none_field_when_not_in_model(db):
    TestNoneNotInsertedController.post({"key": 1, "my_dict": {"null_value": None}})
    assert [{"key": 1, "my_dict": {}}] == TestNoneRetrieveController.get({})
