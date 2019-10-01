import pytest

import layabase
import layabase.database_mongo
import layabase.audit_mongo
from test import DateTimeModuleMock


@pytest.fixture
def controller():
    class TestModel:
        __tablename__ = "test"

        key = layabase.database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = layabase.database_mongo.Column()

    controller = layabase.CRUDController(TestModel, audit=True)
    layabase.load("mongomock", [controller])
    return controller


def test_int_primary_key_is_reset_after_delete(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert controller.post({"other": "test1"}) == {"key": 1, "other": "test1"}
    assert controller.delete({}) == 1
    assert controller.post({"other": "test1"}) == {"key": 1, "other": "test1"}
    assert controller.post({"other": "test1"}) == {"key": 2, "other": "test1"}
    assert controller.get_audit({}) == [
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
