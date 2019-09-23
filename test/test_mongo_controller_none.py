import pytest

from layabase import database, database_mongo


class TestNoneInsertController(database.CRUDController):
    pass


class TestNoneRetrieveController(database.CRUDController):
    pass


class TestNoneNotInsertedController(database.CRUDController):
    pass


def _create_models(base):
    class TestNoneNotInsertedModel(
        database_mongo.CRUDModel, base=base, table_name="none_table_name"
    ):
        key = database_mongo.Column(int, is_primary_key=True)
        my_dict = database_mongo.DictColumn(
            fields={"null_value": database_mongo.Column(store_none=False)},
            is_required=True,
        )

    class TestNoneInsertModel(
        database_mongo.CRUDModel,
        base=base,
        table_name="none_table_name",
        skip_name_check=True,
    ):
        key = database_mongo.Column(int, is_primary_key=True)
        my_dict = database_mongo.DictColumn(
            fields={"null_value": database_mongo.Column(store_none=True)},
            is_required=True,
        )

    class TestNoneRetrieveModel(
        database_mongo.CRUDModel,
        base=base,
        table_name="none_table_name",
        skip_name_check=True,
    ):
        key = database_mongo.Column(int, is_primary_key=True)
        my_dict = database_mongo.Column(dict, is_required=True)

    TestNoneNotInsertedController.model(TestNoneNotInsertedModel)
    TestNoneInsertController.model(TestNoneInsertModel)
    TestNoneRetrieveController.model(TestNoneRetrieveModel)

    return [TestNoneInsertModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    yield _db
    database.reset(_db)


def test_get_retrieve_none_field_when_not_in_model(db):
    TestNoneInsertController.post({"key": 1, "my_dict": {"null_value": None}})
    assert [
        {"key": 1, "my_dict": {"null_value": None}}
    ] == TestNoneRetrieveController.get({})


def test_get_do_not_retrieve_none_field_when_not_in_model(db):
    TestNoneNotInsertedController.post({"key": 1, "my_dict": {"null_value": None}})
    assert [{"key": 1, "my_dict": {}}] == TestNoneRetrieveController.get({})
