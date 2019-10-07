import pytest

import layabase
import layabase.mongo
from layabase.testing import mock_mongo_audit_datetime


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(int)

    controller = layabase.CRUDController(TestCollection, audit=True)
    layabase.load("mongomock", [controller])
    return controller


def test_int_revision_is_not_reset_after_delete(controller, mock_mongo_audit_datetime):
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
