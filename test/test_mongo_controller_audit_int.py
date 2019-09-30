import pytest

import layabase
import layabase.database_mongo
import layabase.testing
import layabase.audit_mongo
from test import DateTimeModuleMock


@pytest.fixture
def controller():
    class TestIntModel:
        __tablename__ = "int_table_name"

        key = layabase.database_mongo.Column(int)

    controller = layabase.CRUDController(TestIntModel, audit=True)
    _db = layabase.load("mongomock?ssl=True", [controller], replicaSet="globaldb")
    yield controller
    layabase.testing.reset(_db)


def test_int_revision_is_not_reset_after_delete(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert {"key": 1} == controller.post({"key": 1})
    assert 1 == controller.delete({})
    assert {"key": 1} == controller.post({"key": 1})
    assert {"key": 2} == controller.post({"key": 2})
    assert controller.get_audit({}) == [
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
