import pytest
from layaberr import ValidationFailed

import layabase
import layabase._database_mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase._database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = layabase._database_mongo.Column(int)

    controller = layabase.CRUDController(TestCollection, audit=True, history=True)
    layabase.load("mongomock", [controller])
    return controller


def test_auto_incremented_fields_are_not_incremented_on_post_failure(controller):
    assert controller.post({"other": 1}) == {
        "key": 1,
        "other": 1,
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }

    # Should not increment revision, nor the auto incremented key
    with pytest.raises(ValidationFailed):
        controller.post({"other": "FAILED"})

    assert controller.post({"other": 2}) == {
        "key": 2,
        "other": 2,
        "valid_since_revision": 2,
        "valid_until_revision": -1,
    }


def test_auto_incremented_fields_are_not_incremented_on_multi_post_failure(controller):
    assert controller.post_many([{"other": 1}]) == [
        {"key": 1, "other": 1, "valid_since_revision": 1, "valid_until_revision": -1}
    ]

    # Should not increment revision, nor the auto incremented key
    with pytest.raises(ValidationFailed):
        controller.post_many([{"other": 2}, {"other": "FAILED"}, {"other": 4}])

    assert controller.post_many([{"other": 5}]) == [
        {
            "key": 3,  # For performance reasons, deserialization is performed before checks on other doc (so first valid document incremented the counter)
            "other": 5,
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        }
    ]


def test_auto_incremented_fields_are_not_incremented_on_multi_put_failure(controller):
    assert controller.post_many([{"other": 1}]) == [
        {"key": 1, "other": 1, "valid_since_revision": 1, "valid_until_revision": -1}
    ]

    # Should not increment revision
    with pytest.raises(ValidationFailed):
        controller.put_many([{"other": 1}, {"other": "FAILED"}, {"other": 1}])

    assert controller.post_many([{"other": 5}]) == [
        {"key": 2, "other": 5, "valid_since_revision": 2, "valid_until_revision": -1}
    ]
