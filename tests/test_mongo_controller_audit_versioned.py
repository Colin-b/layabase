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
        mandatory = layabase.mongo.Column(int, is_nullable=False)
        optional = layabase.mongo.Column(str)

    return layabase.CRUDController(TestCollection, audit=True)


@pytest.fixture
def controller_versioned() -> layabase.CRUDController:
    class TestCollectionVersioned:
        __collection_name__ = "test_versioned"

        key = layabase.mongo.Column(str, is_primary_key=True)
        enum_fld = layabase.mongo.Column(EnumTest)

    return layabase.CRUDController(TestCollectionVersioned, audit=True, history=True)


@pytest.fixture
def controllers(controller: layabase.CRUDController, controller_versioned: layabase.CRUDController):
    return layabase.load("mongomock", [controller, controller_versioned])


def test_revision_not_shared_if_not_versioned(
    controllers,
    controller,
    controller_versioned: layabase.CRUDController,
    mock_mongo_audit_datetime,
):
    assert {"optional": None, "mandatory": 1, "key": "my_key"} == controller.post(
        {"key": "my_key", "mandatory": 1}
    )
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]
    assert controller_versioned.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 1,
            "table_name": "test_versioned",
        }
    ]


def test_revision_on_versioned_audit_after_put_failure(
    controllers,
    controller_versioned: layabase.CRUDController,
    mock_mongo_audit_datetime,
):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    with pytest.raises(layabase.ValidationFailed):
        controller_versioned.put({"key": "my_key2", "enum_fld": EnumTest.Value2})
    controller_versioned.delete({"key": "my_key"})
    assert controller_versioned.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 1,
            "table_name": "test_versioned",
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 2,
            "table_name": "test_versioned",
        },
    ]


def test_get_versioned_audit_after_post_put(
    controllers,
    controller_versioned: layabase.CRUDController,
    mock_mongo_audit_datetime,
):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller_versioned.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    assert controller_versioned.get_one({"key": "my_key"}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": -1,
    }


def test_update_index(controllers, controller_versioned: layabase.CRUDController):
    # Assert no error is thrown
    controller_versioned._model.update_indexes()


def test_post_and_put_many(
    controllers,
    controller_versioned: layabase.CRUDController,
    mock_mongo_audit_datetime,
):
    controller_versioned.post_many(
        [
            {"key": "my_key1", "enum_fld": EnumTest.Value1},
            {"key": "my_key2", "enum_fld": EnumTest.Value1},
        ]
    )
    assert controller_versioned.put_many(
        [
            {"key": "my_key1", "enum_fld": EnumTest.Value2},
            {"key": "my_key2", "enum_fld": EnumTest.Value2},
        ]
    ) == (
        [
            {
                "enum_fld": "Value1",
                "key": "my_key1",
                "valid_since_revision": 1,
                "valid_until_revision": -1,
            },
            {
                "enum_fld": "Value1",
                "key": "my_key2",
                "valid_since_revision": 1,
                "valid_until_revision": -1,
            },
        ],
        [
            {
                "enum_fld": "Value2",
                "key": "my_key1",
                "valid_since_revision": 2,
                "valid_until_revision": -1,
            },
            {
                "enum_fld": "Value2",
                "key": "my_key2",
                "valid_since_revision": 2,
                "valid_until_revision": -1,
            },
        ],
    )


def test_versioned_audit_after_post_put_delete_rollback(
    controllers,
    controller_versioned: layabase.CRUDController,
    mock_mongo_audit_datetime,
):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller_versioned.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    controller_versioned.delete({"key": "my_key"})
    controller_versioned.rollback_to({"revision": 1})
    assert controller_versioned.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 1,
            "table_name": "test_versioned",
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 2,
            "table_name": "test_versioned",
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 3,
            "table_name": "test_versioned",
        },
        {
            "audit_action": "Rollback",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 4,
            "table_name": "test_versioned",
        },
    ]


def test_get_last_when_empty(controllers, controller_versioned):
    assert controller_versioned.get_last({}) == {}


def test_get_last_when_single_doc_post(controllers, controller_versioned):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    assert controller_versioned.get_last({}) == {
        "enum_fld": "Value1",
        "key": "my_key",
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }


def test_get_last_with_unmatched_filter(controllers, controller_versioned):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    assert controller_versioned.get_last({"key": "my_key2"}) == {}


def test_get_last_when_single_update(controllers, controller_versioned):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller_versioned.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    assert controller_versioned.get_last({}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": -1,
    }


def test_get_last_when_removed(controllers, controller_versioned):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller_versioned.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    controller_versioned.delete({"key": "my_key"})
    assert controller_versioned.get_last({}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": 3,
    }


def test_get_last_with_one_removed_and_a_valid(controllers, controller_versioned):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller_versioned.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    controller_versioned.delete({"key": "my_key"})
    controller_versioned.post({"key": "my_key2", "enum_fld": EnumTest.Value1})
    assert controller_versioned.get_last({}) == {
        "enum_fld": "Value1",
        "key": "my_key2",
        "valid_since_revision": 4,
        "valid_until_revision": -1,
    }


def test_get_last_with_one_removed_and_a_valid_and_filter_on_removed(
    controllers, controller_versioned
):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller_versioned.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    controller_versioned.delete({"key": "my_key"})
    controller_versioned.post({"key": "my_key2", "enum_fld": EnumTest.Value1})
    assert controller_versioned.get_last({"key": "my_key"}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": 3,
    }
