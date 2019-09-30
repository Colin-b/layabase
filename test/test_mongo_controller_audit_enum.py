import enum

import pytest

from layabase import database, database_mongo
import layabase.testing
import layabase.audit_mongo
from test import DateTimeModuleMock


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestEnumController(database.CRUDController):
    class TestEnumModel:
        __tablename__ = "enum_table_name"

        key = database_mongo.Column(str, is_primary_key=True)
        enum_fld = database_mongo.Column(EnumTest)

    model = TestEnumModel
    audit = True


@pytest.fixture
def db():
    _db = database.load(
        "mongomock?ssl=True", [TestEnumController], replicaSet="globaldb"
    )
    yield _db
    layabase.testing.reset(_db)


def test_post_with_enum_is_valid(db, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert TestEnumController.post({"key": "my_key", "enum_fld": EnumTest.Value1}) == {
        "enum_fld": "Value1",
        "key": "my_key",
    }
    assert TestEnumController.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "enum_fld": "Value1",
            "key": "my_key",
            "revision": 1,
        }
    ]


def test_put_with_enum_is_valid(db, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert TestEnumController.post({"key": "my_key", "enum_fld": EnumTest.Value1}) == {
        "enum_fld": "Value1",
        "key": "my_key",
    }
    assert TestEnumController.put({"key": "my_key", "enum_fld": EnumTest.Value2}) == (
        {"enum_fld": "Value1", "key": "my_key"},
        {"enum_fld": "Value2", "key": "my_key"},
    )
    assert TestEnumController.get_audit({}) == [
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


def test_delete_with_enum_is_valid(db, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert TestEnumController.post({"key": "my_key", "enum_fld": EnumTest.Value1}) == {
        "enum_fld": "Value1",
        "key": "my_key",
    }
    assert TestEnumController.delete({"enum_fld": EnumTest.Value1}) == 1
    assert TestEnumController.get_audit({}) == [
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
