import enum

import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo, versioning_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestDictRequiredNonNullableVersionedController(database.CRUDController):
    class TestDictRequiredNonNullableVersionedModel:
        __tablename__ = "req_not_null_versioned_table_name"

        key = database_mongo.Column(is_primary_key=True)
        dict_field = database_mongo.DictColumn(
            fields={
                "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                "second_key": database_mongo.Column(int, is_nullable=False),
            },
            is_required=True,
            is_nullable=False,
        )

    model = TestDictRequiredNonNullableVersionedModel
    history = True


@pytest.fixture
def db():
    _db = database.load("mongomock", [TestDictRequiredNonNullableVersionedController])
    yield _db
    layabase.testing.reset(_db)


def test_post_without_providing_required_non_nullable_dict_column_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDictRequiredNonNullableVersionedController.post({"key": "first"})
    assert {
        "dict_field": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "first"} == exception_info.value.received_data


def test_put_without_providing_required_non_nullable_dict_column_is_valid(db):
    TestDictRequiredNonNullableVersionedController.post(
        {"key": "first", "dict_field": {"first_key": "Value1", "second_key": 0}}
    )
    assert (
        {
            "dict_field": {"first_key": "Value1", "second_key": 0},
            "key": "first",
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 0},
            "key": "first",
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
    ) == TestDictRequiredNonNullableVersionedController.put({"key": "first"})


def test_put_with_null_provided_required_non_nullable_dict_column_is_invalid(db):
    TestDictRequiredNonNullableVersionedController.post(
        {"key": "first", "dict_field": {"first_key": "Value1", "second_key": 0}}
    )
    with pytest.raises(ValidationFailed) as exception_info:
        TestDictRequiredNonNullableVersionedController.put(
            {"key": "first", "dict_field": None}
        )
    assert {
        "dict_field": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "first", "dict_field": None} == exception_info.value.received_data
