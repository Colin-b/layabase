import re

import pytest

from layabase import database, database_mongo


class TestPrimaryIntController(database.CRUDController):
    pass


def _create_models(base):
    class TestPrimaryIntModel(
        database_mongo.CRUDModel,
        base=base,
        table_name="prim_int_table_name",
        audit=True,
    ):
        key = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = database_mongo.Column()

    TestPrimaryIntController.model(TestPrimaryIntModel)
    return [TestPrimaryIntModel]


@pytest.fixture
def db():
    _db = database.load("mongomock?ssl=True", _create_models, replicaSet="globaldb")
    yield _db
    database.reset(_db)


def test_int_primary_key_is_reset_after_delete(db):
    assert TestPrimaryIntController.post({"other": "test1"}) == {
        "key": 1,
        "other": "test1",
    }
    assert TestPrimaryIntController.delete({}) == 1
    assert TestPrimaryIntController.post({"other": "test1"}) == {
        "key": 1,
        "other": "test1",
    }
    assert TestPrimaryIntController.post({"other": "test1"}) == {
        "key": 2,
        "other": "test1",
    }
    _check_audit(
        TestPrimaryIntController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "other": "test1",
                "revision": 1,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "other": "test1",
                "revision": 2,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "other": "test1",
                "revision": 3,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 2,
                "other": "test1",
                "revision": 4,
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
