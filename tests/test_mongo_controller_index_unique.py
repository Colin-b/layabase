import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        id = layabase.mongo.Column(index_type=layabase.mongo.IndexType.Unique)
        id2 = layabase.mongo.Column(index_type=layabase.mongo.IndexType.Unique)

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_put_many_without_primary_key_and_unique_index_update(
    controller: layabase.CRUDController,
):
    assert controller.post_many(
        [{"id": "test1", "id2": "test1"}, {"id": "test1", "id2": "test2"}]
    ) == [{"id": "test1", "id2": "test1"}, {"id": "test1", "id2": "test2"}]
    # It should never be declared without a PK in this case but as there is no PK, the first document is updated.
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put_many([{"id2": "test2"}])
    assert exception_info.value.errors == {"": ["One document already exists."]}
    assert exception_info.value.received_data == [{"id": None, "id2": "test2"}]
