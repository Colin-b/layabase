import pytest

import layabase
import layabase._database_mongo
import layabase.testing


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase._database_mongo.Column(str, is_primary_key=True)

    return layabase.CRUDController(TestCollection)


@pytest.fixture
def database(controller):
    return layabase.load("mongomock", [controller])


def test_reset_cleanup_content(controller: layabase.CRUDController, database):
    controller.post_many([{"key": "1"}, {"key": "2"}])
    assert len(controller.get({})) == 2
    layabase.testing.reset(database)
    assert len(controller.get({})) == 0
