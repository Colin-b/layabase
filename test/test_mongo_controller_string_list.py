import pytest

from layabase import database, database_mongo
from test.flask_restplus_mock import TestAPI


class TestStringListController(database.CRUDController):
    pass


def _create_models(base):
    class TestStringListModel(
        database_mongo.CRUDModel, base=base, table_name="string_list_table_name"
    ):
        key = database_mongo.Column(is_primary_key=True)
        list_field = database_mongo.ListColumn(database_mongo.Column(), sorted=True)

    TestStringListController.model(TestStringListModel)

    return [TestStringListModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    TestStringListController.namespace(TestAPI)

    yield _db

    database.reset(_db)


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
