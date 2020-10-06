import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(is_primary_key=True, default_value="test")
        optional = layabase.mongo.Column()

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_post_without_primary_key_but_default_value_is_valid(controller: layabase.CRUDController):
    assert {"key": "test", "optional": "test2"} == controller.post(
        {"optional": "test2"}
    )


def test_get_on_default_value_is_valid(controller: layabase.CRUDController):
    controller.post({"optional": "test"})
    controller.post({"key": "test2", "optional": "test2"})
    assert [{"key": "test", "optional": "test"}] == controller.get({"key": "test"})


def test_delete_on_default_value_is_valid(controller: layabase.CRUDController):
    controller.post({"optional": "test"})
    controller.post({"key": "test2", "optional": "test2"})
    assert 1 == controller.delete({"key": "test"})


def test_put_without_primary_key_but_default_value_is_valid(controller: layabase.CRUDController):
    assert {"key": "test", "optional": "test2"} == controller.post(
        {"optional": "test2"}
    )
    assert (
        {"key": "test", "optional": "test2"},
        {"key": "test", "optional": "test3"},
    ) == controller.put({"optional": "test3"})
