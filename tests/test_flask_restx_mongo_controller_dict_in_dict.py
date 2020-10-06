import enum

import flask
import flask_restx
import pytest

import layabase
import layabase.mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(is_primary_key=True)
        dict_field = layabase.mongo.DictColumn(
            fields={
                "first_key": layabase.mongo.DictColumn(
                    fields={
                        "inner_key1": layabase.mongo.Column(
                            EnumTest, is_nullable=False
                        ),
                        "inner_key2": layabase.mongo.Column(int, is_nullable=False),
                    },
                    is_required=True,
                ),
                "second_key": layabase.mongo.Column(int, is_nullable=False),
            },
            is_required=True,
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


@pytest.fixture
def app(controller: layabase.CRUDController):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restx.Api(application)
    namespace = api.namespace("Test", path="/")

    controller.flask_restx.init_models(namespace)

    @namespace.route("/test")
    class TestResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_get_parser)
        @namespace.marshal_with(controller.flask_restx.get_response_model)
        def get(self):
            return []

        @namespace.expect(controller.flask_restx.json_post_model)
        def post(self):
            return []

        @namespace.expect(controller.flask_restx.json_put_model)
        def put(self):
            return []

        @namespace.expect(controller.flask_restx.query_delete_parser)
        def delete(self):
            return []

    return application


def test_open_api_definition(client):
    response = client.get("/swagger.json")
    assert response.json == {
        "swagger": "2.0",
        "basePath": "/",
        "paths": {
            "/test": {
                "put": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "put_test_resource",
                    "parameters": [
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": {
                                "$ref": "#/definitions/TestCollection_PutRequestModel"
                            },
                        }
                    ],
                    "tags": ["Test"],
                },
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestCollection_GetResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_resource",
                    "parameters": [
                        {
                            "name": "key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field.first_key.inner_key1",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field.first_key.inner_key2",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field.second_key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
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
                            "name": "key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field.first_key.inner_key1",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field.first_key.inner_key2",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field.second_key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
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
                            "schema": {
                                "$ref": "#/definitions/TestCollection_PostRequestModel"
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
            "TestCollection_PutRequestModel": {
                "required": ["dict_field"],
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "dict_field": {
                        "readOnly": False,
                        "default": {
                            "first_key": {"inner_key1": None, "inner_key2": None},
                            "second_key": None,
                        },
                        "example": {
                            "first_key": {"inner_key1": "Value1", "inner_key2": 1},
                            "second_key": 1,
                        },
                        "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                    },
                },
                "type": "object",
            },
            "first_key_second_key": {
                "required": ["first_key"],
                "properties": {
                    "first_key": {
                        "readOnly": False,
                        "default": {"inner_key1": None, "inner_key2": None},
                        "example": {"inner_key1": "Value1", "inner_key2": 1},
                        "allOf": [{"$ref": "#/definitions/inner_key1_inner_key2"}],
                    },
                    "second_key": {"type": "integer", "readOnly": False, "example": 1},
                },
                "type": "object",
            },
            "inner_key1_inner_key2": {
                "properties": {
                    "inner_key1": {
                        "type": "string",
                        "readOnly": False,
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
                    },
                    "inner_key2": {"type": "integer", "readOnly": False, "example": 1},
                },
                "type": "object",
            },
            "TestCollection_PostRequestModel": {
                "required": ["dict_field"],
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "dict_field": {
                        "readOnly": False,
                        "default": {
                            "first_key": {"inner_key1": None, "inner_key2": None},
                            "second_key": None,
                        },
                        "example": {
                            "first_key": {"inner_key1": "Value1", "inner_key2": 1},
                            "second_key": 1,
                        },
                        "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                    },
                },
                "type": "object",
            },
            "TestCollection_GetResponseModel": {
                "required": ["dict_field"],
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "dict_field": {
                        "readOnly": False,
                        "default": {
                            "first_key": {"inner_key1": None, "inner_key2": None},
                            "second_key": None,
                        },
                        "example": {
                            "first_key": {"inner_key1": "Value1", "inner_key2": 1},
                            "second_key": 1,
                        },
                        "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
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
