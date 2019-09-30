import pytest

import layabase
import layabase.database_mongo
import layabase.testing


@pytest.fixture
def controller():
    class TestStringListModel:
        __tablename__ = "test"

        key = layabase.database_mongo.Column(is_primary_key=True)
        list_field = layabase.database_mongo.ListColumn(
            layabase.database_mongo.Column(), sorted=True
        )

    controller = layabase.CRUDController(TestStringListModel)
    _db = layabase.load("mongomock", [controller])
    yield controller
    layabase.testing.reset(_db)


def test_post_list_of_str_is_sorted(controller):
    assert controller.post({"key": "my_key", "list_field": ["c", "a", "b"]}) == {
        "key": "my_key",
        "list_field": ["a", "b", "c"],
    }


def test_put_list_of_str_is_sorted(controller):
    controller.post({"key": "my_key", "list_field": ["a", "c", "b"]})
    assert controller.put({"key": "my_key", "list_field": ["f", "e", "d"]}) == (
        {"key": "my_key", "list_field": ["a", "b", "c"]},
        {"key": "my_key", "list_field": ["d", "e", "f"]},
    )
