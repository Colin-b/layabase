import pytest
import sqlalchemy

from layabase import database, database_sqlalchemy
from test.flask_restplus_mock import TestAPI


class TestAutoIncrementController(database.CRUDController):
    pass


def _create_models(base):
    class TestAutoIncrementModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "auto_increment_table_name"

        key = sqlalchemy.Column(
            sqlalchemy.Integer, primary_key=True, autoincrement=True
        )
        enum_field = sqlalchemy.Column(
            sqlalchemy.Enum("Value1", "Value2"),
            nullable=False,
            doc="Test Documentation",
        )
        optional_with_default = sqlalchemy.Column(
            sqlalchemy.String, default="Test value"
        )

    TestAutoIncrementController.model(TestAutoIncrementModel)
    return [TestAutoIncrementModel]


@pytest.fixture
def db():
    _db = database.load("sqlite:///:memory:", _create_models)
    TestAutoIncrementController.namespace(TestAPI)
    yield _db
    database.reset(_db)


def test_post_with_specified_incremented_field_is_ignored_and_valid(db):
    assert {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    } == TestAutoIncrementController.post({"key": "my_key", "enum_field": "Value1"})


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
