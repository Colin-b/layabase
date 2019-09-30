import pytest

from layabase import database, database_mongo
import layabase.testing


class TestDefaultPrimaryKeyController(database.CRUDController):
    class TestDefaultPrimaryKeyModel:
        __tablename__ = "default_primary_table_name"

        key = database_mongo.Column(is_primary_key=True, default_value="test")
        optional = database_mongo.Column()

    model = TestDefaultPrimaryKeyModel


@pytest.fixture
def db():
    _db = database.load("mongomock", [TestDefaultPrimaryKeyController])
    yield _db
    layabase.testing.reset(_db)


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
