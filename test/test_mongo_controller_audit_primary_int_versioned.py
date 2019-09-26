import pytest

from layabase import database, database_mongo, versioning_mongo
import layabase.testing
import layabase.audit_mongo
from test import DateTimeModuleMock


class TestPrimaryIntVersionedController(database.CRUDController):
    pass


def _create_models(base):
    class TestPrimaryIntVersionedModel(
        versioning_mongo.VersionedCRUDModel,
        base=base,
        table_name="prim_int_version_table_name",
        audit=True,
    ):
        key = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = database_mongo.Column()

    TestPrimaryIntVersionedController.model(TestPrimaryIntVersionedModel)
    return [TestPrimaryIntVersionedModel]


@pytest.fixture
def db():
    _db = database.load("mongomock?ssl=True", _create_models, replicaSet="globaldb")
    yield _db
    layabase.testing.reset(_db)


def test_versioned_int_primary_key_is_reset_after_delete(db, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert TestPrimaryIntVersionedController.post({"other": "test1"}) == {
        "key": 1,
        "other": "test1",
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }
    assert TestPrimaryIntVersionedController.delete({}) == 1
    assert TestPrimaryIntVersionedController.post({"other": "test1"}) == {
        "key": 1,
        "other": "test1",
        "valid_since_revision": 3,
        "valid_until_revision": -1,
    }
    assert TestPrimaryIntVersionedController.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 1,
            "table_name": "prim_int_version_table_name",
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 2,
            "table_name": "prim_int_version_table_name",
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 3,
            "table_name": "prim_int_version_table_name",
        },
    ]
    assert TestPrimaryIntVersionedController.get_history({}) == [
        {
            "key": 1,
            "other": "test1",
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
        {
            "key": 1,
            "other": "test1",
            "valid_since_revision": 3,
            "valid_until_revision": -1,
        },
    ]
