import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo, versioning_mongo
import layabase.testing


class TestAutoIncAuditVersionedController(database.CRUDController):
    pass


def _create_models(base):
    class TestAutoIncAuditVersionedModel(
        versioning_mongo.VersionedCRUDModel,
        base=base,
        table_name="prim_int_auto_inc_version_table_name",
        audit=True,
    ):
        key = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = database_mongo.Column(int)

    TestAutoIncAuditVersionedController.model(TestAutoIncAuditVersionedModel)
    return [TestAutoIncAuditVersionedModel]


@pytest.fixture
def db():
    _db = database.load("mongomock?ssl=True", _create_models, replicaSet="globaldb")
    yield _db
    layabase.testing.reset(_db)


def test_auto_incremented_fields_are_not_incremented_on_post_failure(db):
    assert TestAutoIncAuditVersionedController.post({"other": 1}) == {
        "key": 1,
        "other": 1,
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }

    # Should not increment revision, nor the auto incremented key
    with pytest.raises(ValidationFailed):
        TestAutoIncAuditVersionedController.post({"other": "FAILED"})

    assert TestAutoIncAuditVersionedController.post({"other": 2}) == {
        "key": 2,
        "other": 2,
        "valid_since_revision": 2,
        "valid_until_revision": -1,
    }


def test_auto_incremented_fields_are_not_incremented_on_multi_post_failure(db):
    assert TestAutoIncAuditVersionedController.post_many([{"other": 1}]) == [
        {"key": 1, "other": 1, "valid_since_revision": 1, "valid_until_revision": -1}
    ]

    # Should not increment revision, nor the auto incremented key
    with pytest.raises(ValidationFailed):
        TestAutoIncAuditVersionedController.post_many(
            [{"other": 2}, {"other": "FAILED"}, {"other": 4}]
        )

    assert TestAutoIncAuditVersionedController.post_many([{"other": 5}]) == [
        {
            "key": 3,  # For performance reasons, deserialization is performed before checks on other doc (so first valid document incremented the counter)
            "other": 5,
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        }
    ]


def test_auto_incremented_fields_are_not_incremented_on_multi_put_failure(db):
    assert TestAutoIncAuditVersionedController.post_many([{"other": 1}]) == [
        {"key": 1, "other": 1, "valid_since_revision": 1, "valid_until_revision": -1}
    ]

    # Should not increment revision
    with pytest.raises(ValidationFailed):
        TestAutoIncAuditVersionedController.put_many(
            [{"other": 1}, {"other": "FAILED"}, {"other": 1}]
        )

    assert TestAutoIncAuditVersionedController.post_many([{"other": 5}]) == [
        {"key": 2, "other": 5, "valid_since_revision": 2, "valid_until_revision": -1}
    ]
