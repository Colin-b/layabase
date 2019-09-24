import re

import pytest

from layabase import database, database_mongo, versioning_mongo


class TestPrimaryIntVersionedController(database.CRUDController):
    pass


def _create_models(base):
    class TestPrimaryIntVersionedModel(
        versioning_mongo.VersionedCRUDModel,
        base=base,
        table_name="prim_int_version_table_name",
        audit=True,
    ):
        key = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = database_mongo.Column()

    TestPrimaryIntVersionedController.model(TestPrimaryIntVersionedModel)
    return [TestPrimaryIntVersionedModel]


@pytest.fixture
def db():
    _db = database.load("mongomock?ssl=True", _create_models, replicaSet="globaldb")
    yield _db
    database.reset(_db)


def test_versioned_int_primary_key_is_reset_after_delete(db):
    assert TestPrimaryIntVersionedController.post({"other": "test1"}) == {
        "key": 1,
        "other": "test1",
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }
    assert TestPrimaryIntVersionedController.delete({}) == 1
    assert TestPrimaryIntVersionedController.post({"other": "test1"}) == {
        "key": 1,
        "other": "test1",
        "valid_since_revision": 3,
        "valid_until_revision": -1,
    }
    _check_audit(
        TestPrimaryIntVersionedController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 1,
                "table_name": "prim_int_version_table_name",
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 2,
                "table_name": "prim_int_version_table_name",
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 3,
                "table_name": "prim_int_version_table_name",
            },
        ],
    )
    assert TestPrimaryIntVersionedController.get_history({}) == [
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
