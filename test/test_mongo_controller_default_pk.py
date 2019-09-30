import pytest

import layabase
import layabase.database_mongo
import layabase.testing


@pytest.fixture
def controller():
    class TestDefaultPrimaryKeyModel:
        __tablename__ = "test"

        key = layabase.database_mongo.Column(is_primary_key=True, default_value="test")
        optional = layabase.database_mongo.Column()

    controller = layabase.CRUDController(TestDefaultPrimaryKeyModel)
    _db = layabase.load("mongomock", [controller])
    yield controller
    layabase.testing.reset(_db)


def test_post_without_primary_key_but_default_value_is_valid(controller):
    assert {"key": "test", "optional": "test2"} == controller.post(
        {"optional": "test2"}
    )


def test_get_on_default_value_is_valid(controller):
    controller.post({"optional": "test"})
    controller.post({"key": "test2", "optional": "test2"})
    assert [{"key": "test", "optional": "test"}] == controller.get({"key": "test"})


def test_delete_on_default_value_is_valid(controller):
    controller.post({"optional": "test"})
    controller.post({"key": "test2", "optional": "test2"})
    assert 1 == controller.delete({"key": "test"})


def test_put_without_primary_key_but_default_value_is_valid(controller):
    assert {"key": "test", "optional": "test2"} == controller.post(
        {"optional": "test2"}
    )
    assert (
        {"key": "test", "optional": "test2"},
        {"key": "test", "optional": "test3"},
    ) == controller.put({"optional": "test3"})
