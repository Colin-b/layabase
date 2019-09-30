import pytest

from layabase import database, database_mongo
import layabase.testing


class TestStringListController(database.CRUDController):
    class TestStringListModel:
        __tablename__ = "string_list_table_name"

        key = database_mongo.Column(is_primary_key=True)
        list_field = database_mongo.ListColumn(database_mongo.Column(), sorted=True)

    model = TestStringListModel


@pytest.fixture
def db():
    _db = database.load("mongomock", [TestStringListController])
    yield _db
    layabase.testing.reset(_db)


def test_post_list_of_str_is_sorted(db):
    assert {
        "key": "my_key",
        "list_field": ["a", "b", "c"],
    } == TestStringListController.post({"key": "my_key", "list_field": ["c", "a", "b"]})


def test_put_list_of_str_is_sorted(db):
    TestStringListController.post({"key": "my_key", "list_field": ["a", "c", "b"]})
    assert (
        {"key": "my_key", "list_field": ["a", "b", "c"]},
        {"key": "my_key", "list_field": ["d", "e", "f"]},
    ) == TestStringListController.put({"key": "my_key", "list_field": ["f", "e", "d"]})
