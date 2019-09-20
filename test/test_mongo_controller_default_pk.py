import pytest

from layabase import database, database_mongo
from test.flask_restplus_mock import TestAPI


class TestDefaultPrimaryKeyController(database.CRUDController):
    pass


def _create_models(base):
    class TestDefaultPrimaryKeyModel(
        database_mongo.CRUDModel, base=base, table_name="default_primary_table_name"
    ):
        key = database_mongo.Column(is_primary_key=True, default_value="test")
        optional = database_mongo.Column()

    TestDefaultPrimaryKeyController.model(TestDefaultPrimaryKeyModel)

    return [TestDefaultPrimaryKeyModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    TestDefaultPrimaryKeyController.namespace(TestAPI)

    yield _db

    database.reset(_db)


def test_post_without_primary_key_but_default_value_is_valid(db):
    assert {"key": "test", "optional": "test2"} == TestDefaultPrimaryKeyController.post(
        {"optional": "test2"}
    )


def test_get_on_default_value_is_valid(db):
    TestDefaultPrimaryKeyController.post({"optional": "test"})
    TestDefaultPrimaryKeyController.post({"key": "test2", "optional": "test2"})
    assert [{"key": "test", "optional": "test"}] == TestDefaultPrimaryKeyController.get(
        {"key": "test"}
    )


def test_delete_on_default_value_is_valid(db):
    TestDefaultPrimaryKeyController.post({"optional": "test"})
    TestDefaultPrimaryKeyController.post({"key": "test2", "optional": "test2"})
    assert 1 == TestDefaultPrimaryKeyController.delete({"key": "test"})


def test_put_without_primary_key_but_default_value_is_valid(db):
    assert {"key": "test", "optional": "test2"} == TestDefaultPrimaryKeyController.post(
        {"optional": "test2"}
    )
    assert (
        {"key": "test", "optional": "test2"},
        {"key": "test", "optional": "test3"},
    ) == TestDefaultPrimaryKeyController.put({"optional": "test3"})
