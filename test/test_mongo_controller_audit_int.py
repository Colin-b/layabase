import pytest

from layabase import database, database_mongo
import layabase.testing
import layabase.audit_mongo
from test import DateTimeModuleMock


class TestIntController(database.CRUDController):
    pass


def _create_models(base):
    class TestIntModel(
        database_mongo.CRUDModel, base=base, table_name="int_table_name", audit=True
    ):
        key = database_mongo.Column(int)

    TestIntController.model(TestIntModel)
    return [TestIntModel]


@pytest.fixture
def db():
    _db = database.load("mongomock?ssl=True", _create_models, replicaSet="globaldb")
    yield _db
    layabase.testing.reset(_db)


def test_int_revision_is_not_reset_after_delete(db, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert {"key": 1} == TestIntController.post({"key": 1})
    assert 1 == TestIntController.delete({})
    assert {"key": 1} == TestIntController.post({"key": 1})
    assert {"key": 2} == TestIntController.post({"key": 2})
    assert TestIntController.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": 1,
            "revision": 1,
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": 1,
            "revision": 2,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": 1,
            "revision": 3,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": 2,
            "revision": 4,
        },
    ]
