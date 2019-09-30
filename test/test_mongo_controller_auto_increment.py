import enum

import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed

import layabase
import layabase.database_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller():
    class TestController(layabase.CRUDController):
        class TestAutoIncrementModel:
            __tablename__ = "auto_increment_table_name"

            key = layabase.database_mongo.Column(
                int, is_primary_key=True, should_auto_increment=True
            )
            enum_field = layabase.database_mongo.Column(
                EnumTest, is_nullable=False, description="Test Documentation"
            )
            optional_with_default = layabase.database_mongo.Column(
                str, default_value="Test value"
            )

        model = TestAutoIncrementModel

    _db = layabase.load("mongomock", [TestController])
    yield TestController
    layabase.testing.reset(_db)


@pytest.fixture
def app(controller):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    controller.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(controller.query_get_parser)
        @namespace.marshal_with(controller.get_response_model)
        def get(self):
            return []

        @namespace.expect(controller.json_post_model)
        def post(self):
            return []

        @namespace.expect(controller.json_put_model)
        def put(self):
            return []

        @namespace.expect(controller.query_delete_parser)
        def delete(self):
            return []

    return application


def test_post_with_specified_incremented_field_is_ignored_and_valid(client, controller):
    assert controller.post({"key": "my_key", "enum_field": "Value1"}) == {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    }


def test_post_with_enum_is_valid(client, controller):
    assert controller.post({"key": "my_key", "enum_field": EnumTest.Value1}) == {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    }


def test_post_with_invalid_enum_choice_is_invalid(client, controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"key": "my_key", "enum_field": "InvalidValue"})
    assert exception_info.value.errors == {
        "enum_field": ["Value \"InvalidValue\" is not within ['Value1', 'Value2']."]
    }
    assert exception_info.value.received_data == {"enum_field": "InvalidValue"}


def test_post_many_with_specified_incremented_field_is_ignored_and_valid(
    client, controller
):
    assert controller.post_many(
        [
            {"key": "my_key", "enum_field": "Value1"},
            {"key": "my_key", "enum_field": "Value2"},
        ]
    ) == [
        {"optional_with_default": "Test value", "enum_field": "Value1", "key": 1},
        {"optional_with_default": "Test value", "enum_field": "Value2", "key": 2},
    ]


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
                            "schema": {
                                "$ref": "#/definitions/TestAutoIncrementModel_GetResponseModel"
                            },
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
                "put": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "put_test_resource",
                    "parameters": [
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": {
                                "$ref": "#/definitions/TestAutoIncrementModel_PutRequestModel"
                            },
                        }
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
                            "schema": {
                                "$ref": "#/definitions/TestAutoIncrementModel_PostRequestModel"
                            },
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
            "TestAutoIncrementModel_PutRequestModel": {
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
            },
            "TestAutoIncrementModel_PostRequestModel": {
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
            },
            "TestAutoIncrementModel_GetResponseModel": {
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
            },
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }
