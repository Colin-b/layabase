import enum
import re

import pytest

from layabase import database, database_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestEnumController(database.CRUDController):
    pass


def _create_models(base):
    class TestEnumModel(
        database_mongo.CRUDModel, base=base, table_name="enum_table_name", audit=True
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        enum_fld = database_mongo.Column(EnumTest)

    TestEnumController.model(TestEnumModel)
    return [TestEnumModel]


@pytest.fixture
def db():
    _db = database.load("mongomock?ssl=True", _create_models, replicaSet="globaldb")
    yield _db
    layabase.testing.reset(_db)


def test_post_with_enum_is_valid(db):
    assert {"enum_fld": "Value1", "key": "my_key"} == TestEnumController.post(
        {"key": "my_key", "enum_fld": EnumTest.Value1}
    )
    _check_audit(
        TestEnumController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "enum_fld": "Value1",
                "key": "my_key",
                "revision": 1,
            }
        ],
    )


def test_put_with_enum_is_valid(db):
    assert {"enum_fld": "Value1", "key": "my_key"} == TestEnumController.post(
        {"key": "my_key", "enum_fld": EnumTest.Value1}
    )
    assert (
        {"enum_fld": "Value1", "key": "my_key"},
        {"enum_fld": "Value2", "key": "my_key"},
    ) == TestEnumController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    _check_audit(
        TestEnumController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "enum_fld": "Value1",
                "key": "my_key",
                "revision": 1,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "enum_fld": "Value2",
                "key": "my_key",
                "revision": 2,
            },
        ],
    )


def test_delete_with_enum_is_valid(db):
    assert {"enum_fld": "Value1", "key": "my_key"} == TestEnumController.post(
        {"key": "my_key", "enum_fld": EnumTest.Value1}
    )
    assert 1 == TestEnumController.delete({"enum_fld": EnumTest.Value1})
    _check_audit(
        TestEnumController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "enum_fld": "Value1",
                "key": "my_key",
                "revision": 1,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "enum_fld": "Value1",
                "key": "my_key",
                "revision": 2,
            },
        ],
    )


def _check_audit(controller, expected_audit):
    audit = controller.get_audit({})
    audit = [
        {key: audit_line[key] for key in sorted(audit_line.keys())}
        for audit_line in audit
    ]

    assert re.match(
        f"{expected_audit}".replace("[", "\\[")
        .replace("]", "\\]")
        .replace("\\\\", "\\"),
        f"{audit}",
    )
