import pytest

from layabase import database, database_mongo
import layabase.testing
import layabase.audit_mongo
from test import DateTimeModuleMock


class TestPrimaryIntController(database.CRUDController):
    pass


def _create_models(base):
    class TestPrimaryIntModel(
        database_mongo.CRUDModel,
        base=base,
        table_name="prim_int_table_name",
        audit=True,
    ):
        key = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = database_mongo.Column()

    TestPrimaryIntController.model(TestPrimaryIntModel)
    return [TestPrimaryIntModel]


@pytest.fixture
def db():
    _db = database.load("mongomock?ssl=True", _create_models, replicaSet="globaldb")
    yield _db
    layabase.testing.reset(_db)


def test_int_primary_key_is_reset_after_delete(db, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert TestPrimaryIntController.post({"other": "test1"}) == {
        "key": 1,
        "other": "test1",
    }
    assert TestPrimaryIntController.delete({}) == 1
    assert TestPrimaryIntController.post({"other": "test1"}) == {
        "key": 1,
        "other": "test1",
    }
    assert TestPrimaryIntController.post({"other": "test1"}) == {
        "key": 2,
        "other": "test1",
    }
    assert TestPrimaryIntController.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": 1,
            "other": "test1",
            "revision": 1,
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": 1,
            "other": "test1",
            "revision": 2,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": 1,
            "other": "test1",
            "revision": 3,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": 2,
            "other": "test1",
            "revision": 4,
        },
    ]
