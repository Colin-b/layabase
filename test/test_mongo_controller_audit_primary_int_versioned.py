import pytest

import layabase
import layabase._database_mongo
from layabase.testing import mock_mongo_audit_datetime


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase._database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = layabase._database_mongo.Column()

    controller = layabase.CRUDController(TestCollection, audit=True, history=True)
    layabase.load("mongomock", [controller])
    return controller


def test_versioned_int_primary_key_is_reset_after_delete(
    controller, mock_mongo_audit_datetime
):
    assert controller.post({"other": "test1"}) == {
        "key": 1,
        "other": "test1",
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }
    assert controller.delete({}) == 1
    assert controller.post({"other": "test1"}) == {
        "key": 1,
        "other": "test1",
        "valid_since_revision": 3,
        "valid_until_revision": -1,
    }
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 1,
            "table_name": "test",
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 2,
            "table_name": "test",
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 3,
            "table_name": "test",
        },
    ]
    assert controller.get_history({}) == [
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
