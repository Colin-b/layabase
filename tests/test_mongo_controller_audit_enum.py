import enum

import pytest

import layabase
import layabase.mongo
from layabase.testing import mock_mongo_audit_datetime


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        enum_fld = layabase.mongo.Column(EnumTest)

    controller = layabase.CRUDController(TestCollection, audit=True)
    layabase.load("mongomock", [controller])
    return controller


def test_post_with_enum_is_valid(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    assert controller.post({"key": "my_key", "enum_fld": EnumTest.Value1}) == {
        "enum_fld": "Value1",
        "key": "my_key",
    }
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "enum_fld": "Value1",
            "key": "my_key",
            "revision": 1,
        }
    ]


def test_put_with_enum_is_valid(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    assert controller.post({"key": "my_key", "enum_fld": EnumTest.Value1}) == {
        "enum_fld": "Value1",
        "key": "my_key",
    }
    assert controller.put({"key": "my_key", "enum_fld": EnumTest.Value2}) == (
        {"enum_fld": "Value1", "key": "my_key"},
        {"enum_fld": "Value2", "key": "my_key"},
    )
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "enum_fld": "Value1",
            "key": "my_key",
            "revision": 1,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "enum_fld": "Value2",
            "key": "my_key",
            "revision": 2,
        },
    ]


def test_delete_with_enum_is_valid(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    assert controller.post({"key": "my_key", "enum_fld": EnumTest.Value1}) == {
        "enum_fld": "Value1",
        "key": "my_key",
    }
    assert controller.delete({"enum_fld": EnumTest.Value1}) == 1
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "enum_fld": "Value1",
            "key": "my_key",
            "revision": 1,
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "enum_fld": "Value1",
            "key": "my_key",
            "revision": 2,
        },
    ]
