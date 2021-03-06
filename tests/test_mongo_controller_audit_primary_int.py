import pytest

import layabase
import layabase.mongo
from layabase.testing import mock_mongo_audit_datetime


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = layabase.mongo.Column()

    controller = layabase.CRUDController(TestCollection, audit=True)
    layabase.load("mongomock", [controller])
    return controller


def test_int_primary_key_is_reset_after_delete(controller: layabase.CRUDController, mock_mongo_audit_datetime):
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
