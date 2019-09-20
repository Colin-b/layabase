import enum

import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo
from test.flask_restplus_mock import TestAPI


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestAutoIncrementController(database.CRUDController):
    pass


def _create_models(base):
    class TestAutoIncrementModel(
        database_mongo.CRUDModel, base=base, table_name="auto_increment_table_name"
    ):
        key = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        enum_field = database_mongo.Column(
            EnumTest, is_nullable=False, description="Test Documentation"
        )
        optional_with_default = database_mongo.Column(str, default_value="Test value")

    TestAutoIncrementController.model(TestAutoIncrementModel)

    return [TestAutoIncrementModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    TestAutoIncrementController.namespace(TestAPI)

    yield _db

    database.reset(_db)


class DateTimeModuleMock:
    class DateTimeMock:
        @staticmethod
        def utcnow():
            class UTCDateTimeMock:
                @staticmethod
                def isoformat():
                    return "2018-10-11T15:05:05.663979"

            return UTCDateTimeMock

    datetime = DateTimeMock


def test_post_with_specified_incremented_field_is_ignored_and_valid(db):
    assert {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    } == TestAutoIncrementController.post({"key": "my_key", "enum_field": "Value1"})


def test_post_with_enum_is_valid(db):
    assert {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    } == TestAutoIncrementController.post(
        {"key": "my_key", "enum_field": EnumTest.Value1}
    )


def test_post_with_invalid_enum_choice_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestAutoIncrementController.post(
            {"key": "my_key", "enum_field": "InvalidValue"}
        )
    assert {
        "enum_field": ["Value \"InvalidValue\" is not within ['Value1', 'Value2']."]
    } == exception_info.value.errors
    assert {"enum_field": "InvalidValue"} == exception_info.value.received_data


def test_post_many_with_specified_incremented_field_is_ignored_and_valid(db):
    assert [
        {"optional_with_default": "Test value", "enum_field": "Value1", "key": 1},
        {"optional_with_default": "Test value", "enum_field": "Value2", "key": 2},
    ] == TestAutoIncrementController.post_many(
        [
            {"key": "my_key", "enum_field": "Value1"},
            {"key": "my_key", "enum_field": "Value2"},
        ]
    )


def test_json_post_model_with_auto_increment_and_enum(db):
    assert "TestAutoIncrementModel" == TestAutoIncrementController.json_post_model.name
    assert {
        "enum_field": "String",
        "key": "Integer",
        "optional_with_default": "String",
    } == TestAutoIncrementController.json_post_model.fields_flask_type
    assert {
        "enum_field": None,
        "key": None,
        "optional_with_default": "Test value",
    } == TestAutoIncrementController.json_post_model.fields_default


def test_json_put_model_with_auto_increment_and_enum(db):
    assert "TestAutoIncrementModel" == TestAutoIncrementController.json_put_model.name
    assert {
        "enum_field": "String",
        "key": "Integer",
        "optional_with_default": "String",
    } == TestAutoIncrementController.json_put_model.fields_flask_type


def test_get_response_model_with_enum(db):
    assert (
        "TestAutoIncrementModel" == TestAutoIncrementController.get_response_model.name
    )
    assert {
        "enum_field": "String",
        "key": "Integer",
        "optional_with_default": "String",
    } == TestAutoIncrementController.get_response_model.fields_flask_type
    assert {
        "enum_field": "Test Documentation",
        "key": None,
        "optional_with_default": None,
    } == TestAutoIncrementController.get_response_model.fields_description
    assert {
        "enum_field": ["Value1", "Value2"],
        "key": None,
        "optional_with_default": None,
    } == TestAutoIncrementController.get_response_model.fields_enum
