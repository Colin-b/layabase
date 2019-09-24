import enum

import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo


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
    yield _db
    database.reset(_db)


@pytest.fixture
def app(db):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    TestAutoIncrementController.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(TestAutoIncrementController.query_get_parser)
        @namespace.marshal_with(TestAutoIncrementController.get_response_model)
        def get(self):
            return []

        @namespace.expect(TestAutoIncrementController.json_post_model)
        def post(self):
            return []

        @namespace.expect(TestAutoIncrementController.json_put_model)
        def put(self):
            return []

        @namespace.expect(TestAutoIncrementController.query_delete_parser)
        def delete(self):
            return []

    return application


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


def test_post_with_specified_incremented_field_is_ignored_and_valid(client):
    assert {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    } == TestAutoIncrementController.post({"key": "my_key", "enum_field": "Value1"})


def test_post_with_enum_is_valid(client):
    assert {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    } == TestAutoIncrementController.post(
        {"key": "my_key", "enum_field": EnumTest.Value1}
    )


def test_post_with_invalid_enum_choice_is_invalid(client):
    with pytest.raises(ValidationFailed) as exception_info:
        TestAutoIncrementController.post(
            {"key": "my_key", "enum_field": "InvalidValue"}
        )
    assert {
        "enum_field": ["Value \"InvalidValue\" is not within ['Value1', 'Value2']."]
    } == exception_info.value.errors
    assert {"enum_field": "InvalidValue"} == exception_info.value.received_data


def test_post_many_with_specified_incremented_field_is_ignored_and_valid(client):
    assert [
        {"optional_with_default": "Test value", "enum_field": "Value1", "key": 1},
        {"optional_with_default": "Test value", "enum_field": "Value2", "key": 2},
    ] == TestAutoIncrementController.post_many(
        [
            {"key": "my_key", "enum_field": "Value1"},
            {"key": "my_key", "enum_field": "Value2"},
        ]
    )


def test_open_api_definition(client):
    response = client.get("/swagger.json")
    assert response.json == {
        "swagger": "2.0",
        "basePath": "/",
        "paths": {
            "/test": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {"$ref": "#/definitions/TestAutoIncrementModel"},
                        }
                    },
                    "operationId": "get_test_resource",
                    "parameters": [
                        {
                            "name": "enum_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "optional_with_default",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "limit",
                            "in": "query",
                            "type": "integer",
                            "minimum": 0,
                            "exclusiveMinimum": True,
                        },
                        {
                            "name": "offset",
                            "in": "query",
                            "type": "integer",
                            "minimum": 0,
                        },
                        {
                            "name": "X-Fields",
                            "in": "header",
                            "type": "string",
                            "format": "mask",
                            "description": "An optional fields mask",
                        },
                    ],
                    "tags": ["Test"],
                },
                "put": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "put_test_resource",
                    "parameters": [
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": {"$ref": "#/definitions/TestAutoIncrementModel"},
                        }
                    ],
                    "tags": ["Test"],
                },
                "delete": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "delete_test_resource",
                    "parameters": [
                        {
                            "name": "enum_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "optional_with_default",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                    ],
                    "tags": ["Test"],
                },
                "post": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "post_test_resource",
                    "parameters": [
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": {"$ref": "#/definitions/TestAutoIncrementModel"},
                        }
                    ],
                    "tags": ["Test"],
                },
            }
        },
        "info": {"title": "API", "version": "1.0"},
        "produces": ["application/json"],
        "consumes": ["application/json"],
        "tags": [{"name": "Test"}],
        "definitions": {
            "TestAutoIncrementModel": {
                "properties": {
                    "enum_field": {
                        "type": "string",
                        "description": "Test Documentation",
                        "readOnly": False,
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
                    },
                    "key": {"type": "integer", "readOnly": True, "example": 1},
                    "optional_with_default": {
                        "type": "string",
                        "readOnly": False,
                        "default": "Test value",
                        "example": "Test value",
                    },
                },
                "type": "object",
            }
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }
