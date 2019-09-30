import enum

import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo, versioning_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestVersionedController(database.CRUDController):
    class TestVersionedModel:
        __tablename__ = "versioned_table_name"

        key = database_mongo.Column(is_primary_key=True)
        dict_field = database_mongo.DictColumn(
            fields={
                "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                "second_key": database_mongo.Column(int, is_nullable=False),
            },
            is_required=True,
        )

    model = TestVersionedModel
    history = True


class TestVersionedUniqueNonPrimaryController(database.CRUDController):
    class TestVersionedUniqueNonPrimaryModel:
        __tablename__ = "versioned_uni_table_name"

        key = database_mongo.Column(int, should_auto_increment=True)
        unique = database_mongo.Column(int, index_type=database_mongo.IndexType.Unique)

    model = TestVersionedUniqueNonPrimaryModel
    history = True


class TestUniqueNonPrimaryController(database.CRUDController):
    class TestUniqueNonPrimaryModel:
        __tablename__ = "uni_table_name"

        key = database_mongo.Column(int, should_auto_increment=True)
        unique = database_mongo.Column(int, index_type=database_mongo.IndexType.Unique)

    model = TestUniqueNonPrimaryModel


@pytest.fixture
def db():
    _db = database.load(
        "mongomock",
        [
            TestVersionedController,
            TestUniqueNonPrimaryController,
            TestVersionedUniqueNonPrimaryController,
        ],
    )
    yield _db
    layabase.testing.reset(_db)


def test_get_url_without_primary_key_in_model_and_many_models(db):
    models = [{"key": 1, "unique": 2}, {"key": 2, "unique": 3}]
    assert TestUniqueNonPrimaryController.get_url("/test", *models) == "/test"


def test_get_url_without_primary_key_in_model_and_one_model(db):
    model = {"key": 1, "unique": 2}
    assert TestUniqueNonPrimaryController.get_url("/test", model) == "/test"


def test_get_url_without_primary_key_in_model_and_no_model(db):
    assert "/test" == TestUniqueNonPrimaryController.get_url("/test")


def test_revison_is_shared(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    TestVersionedUniqueNonPrimaryController.post({"unique": 1})
    TestVersionedController.put({"key": "first", "dict_field.second_key": 2})
    TestVersionedController.delete({"key": "first"})
    TestVersionedController.rollback_to({"revision": 2})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 2},
            "valid_since_revision": 3,
            "valid_until_revision": 4,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 3,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 5,
            "valid_until_revision": -1,
        },
    ] == TestVersionedController.get_history({})
    assert [
        {"key": 1, "unique": 1, "valid_since_revision": 2, "valid_until_revision": -1}
    ] == TestVersionedUniqueNonPrimaryController.get_history({})


def test_versioning_handles_unique_non_primary(db):
    TestVersionedUniqueNonPrimaryController.post({"unique": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        TestVersionedUniqueNonPrimaryController.post({"unique": 1})
    assert {"": ["This document already exists."]} == exception_info.value.errors
    assert {
        "key": 2,
        "unique": 1,
        "valid_since_revision": 2,
        "valid_until_revision": -1,
    } == exception_info.value.received_data


def test_insert_to_non_unique_after_update(db):
    TestVersionedUniqueNonPrimaryController.post({"unique": 1})
    TestVersionedUniqueNonPrimaryController.put({"key": 1, "unique": 2})
    with pytest.raises(ValidationFailed) as exception_info:
        TestVersionedUniqueNonPrimaryController.post({"unique": 2})
    assert {"": ["This document already exists."]} == exception_info.value.errors
    assert {
        "key": 2,
        "unique": 2,
        "valid_since_revision": 3,
        "valid_until_revision": -1,
    } == exception_info.value.received_data


def test_update_to_non_unique_versioned(db):
    TestVersionedUniqueNonPrimaryController.post({"unique": 1})
    TestVersionedUniqueNonPrimaryController.post({"unique": 2})
    with pytest.raises(ValidationFailed) as exception_info:
        TestVersionedUniqueNonPrimaryController.put({"key": 1, "unique": 2})
    assert {"": ["This document already exists."]} == exception_info.value.errors
    assert {
        "key": 1,
        "unique": 2,
        "valid_since_revision": 3,
        "valid_until_revision": -1,
    } == exception_info.value.received_data


def test_update_to_non_unique(db):
    TestUniqueNonPrimaryController.post({"unique": 1})
    TestUniqueNonPrimaryController.post({"unique": 2})
    with pytest.raises(ValidationFailed) as exception_info:
        TestUniqueNonPrimaryController.put({"unique": 2, "key": 1})
    assert {"": ["This document already exists."]} == exception_info.value.errors
    assert {"key": 1, "unique": 2} == exception_info.value.received_data
