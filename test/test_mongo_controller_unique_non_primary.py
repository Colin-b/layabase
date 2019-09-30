import enum

import pytest
from layaberr import ValidationFailed

import layabase
import layabase.database_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller_versioned():
    class TestVersionedModel:
        __tablename__ = "versioned_table_name"

        key = layabase.database_mongo.Column(is_primary_key=True)
        dict_field = layabase.database_mongo.DictColumn(
            fields={
                "first_key": layabase.database_mongo.Column(
                    EnumTest, is_nullable=False
                ),
                "second_key": layabase.database_mongo.Column(int, is_nullable=False),
            },
            is_required=True,
        )

    return layabase.CRUDController(TestVersionedModel, history=True)


@pytest.fixture
def controller_versioned_unique():
    class TestVersionedUniqueNonPrimaryModel:
        __tablename__ = "versioned_uni_table_name"

        key = layabase.database_mongo.Column(int, should_auto_increment=True)
        unique = layabase.database_mongo.Column(
            int, index_type=layabase.database_mongo.IndexType.Unique
        )

    return layabase.CRUDController(TestVersionedUniqueNonPrimaryModel, history=True)


@pytest.fixture
def controller_unique():
    class TestUniqueNonPrimaryModel:
        __tablename__ = "uni_table_name"

        key = layabase.database_mongo.Column(int, should_auto_increment=True)
        unique = layabase.database_mongo.Column(
            int, index_type=layabase.database_mongo.IndexType.Unique
        )

    return layabase.CRUDController(TestUniqueNonPrimaryModel)


@pytest.fixture
def controllers(controller_versioned, controller_unique, controller_versioned_unique):
    _db = layabase.load(
        "mongomock",
        [controller_versioned, controller_unique, controller_versioned_unique],
    )
    yield controller_versioned, controller_unique, controller_versioned_unique
    layabase.testing.reset(_db)


def test_get_url_without_primary_key_in_model_and_many_models(
    controllers, controller_unique
):
    models = [{"key": 1, "unique": 2}, {"key": 2, "unique": 3}]
    assert controller_unique.get_url("/test", *models) == "/test"


def test_get_url_without_primary_key_in_model_and_one_model(
    controllers, controller_unique
):
    model = {"key": 1, "unique": 2}
    assert controller_unique.get_url("/test", model) == "/test"


def test_get_url_without_primary_key_in_model_and_no_model(
    controllers, controller_unique
):
    assert controller_unique.get_url("/test") == "/test"


def test_revison_is_shared(
    controllers, controller_versioned, controller_versioned_unique
):
    controller_versioned.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    controller_versioned_unique.post({"unique": 1})
    controller_versioned.put({"key": "first", "dict_field.second_key": 2})
    controller_versioned.delete({"key": "first"})
    controller_versioned.rollback_to({"revision": 2})
    assert controller_versioned.get_history({}) == [
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
    ]
    assert controller_versioned_unique.get_history({}) == [
        {"key": 1, "unique": 1, "valid_since_revision": 2, "valid_until_revision": -1}
    ]


def test_versioning_handles_unique_non_primary(
    controllers, controller_versioned_unique
):
    controller_versioned_unique.post({"unique": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        controller_versioned_unique.post({"unique": 1})
    assert exception_info.value.errors == {"": ["This document already exists."]}
    assert exception_info.value.received_data == {
        "key": 2,
        "unique": 1,
        "valid_since_revision": 2,
        "valid_until_revision": -1,
    }


def test_insert_to_non_unique_after_update(controllers, controller_versioned_unique):
    controller_versioned_unique.post({"unique": 1})
    controller_versioned_unique.put({"key": 1, "unique": 2})
    with pytest.raises(ValidationFailed) as exception_info:
        controller_versioned_unique.post({"unique": 2})
    assert exception_info.value.errors == {"": ["This document already exists."]}
    assert exception_info.value.received_data == {
        "key": 2,
        "unique": 2,
        "valid_since_revision": 3,
        "valid_until_revision": -1,
    }


def test_update_to_non_unique_versioned(controllers, controller_versioned_unique):
    controller_versioned_unique.post({"unique": 1})
    controller_versioned_unique.post({"unique": 2})
    with pytest.raises(ValidationFailed) as exception_info:
        controller_versioned_unique.put({"key": 1, "unique": 2})
    assert exception_info.value.errors == {"": ["This document already exists."]}
    assert exception_info.value.received_data == {
        "key": 1,
        "unique": 2,
        "valid_since_revision": 3,
        "valid_until_revision": -1,
    }


def test_update_to_non_unique(controllers, controller_unique):
    controller_unique.post({"unique": 1})
    controller_unique.post({"unique": 2})
    with pytest.raises(ValidationFailed) as exception_info:
        controller_unique.put({"unique": 2, "key": 1})
    assert exception_info.value.errors == {"": ["This document already exists."]}
    assert exception_info.value.received_data == {"key": 1, "unique": 2}
