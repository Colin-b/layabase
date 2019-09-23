import re

import pytest

from layabase import database, database_mongo


class TestIntController(database.CRUDController):
    pass


def _create_models(base):
    class TestIntModel(
        database_mongo.CRUDModel, base=base, table_name="int_table_name", audit=True
    ):
        key = database_mongo.Column(int)

    TestIntController.model(TestIntModel)
    return [TestIntModel]


@pytest.fixture
def db():
    _db = database.load("mongomock?ssl=True", _create_models, replicaSet="globaldb")
    yield _db
    database.reset(_db)


def test_int_revision_is_not_reset_after_delete(db):
    assert {"key": 1} == TestIntController.post({"key": 1})
    assert 1 == TestIntController.delete({})
    assert {"key": 1} == TestIntController.post({"key": 1})
    assert {"key": 2} == TestIntController.post({"key": 2})
    _check_audit(
        TestIntController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "revision": 1,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "revision": 2,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "revision": 3,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 2,
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
