import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(is_primary_key=True)
        list_field = layabase.mongo.ListColumn(layabase.mongo.Column(), sorted=True)

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


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
