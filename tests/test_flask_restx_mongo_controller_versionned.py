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
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(is_primary_key=True)
        dict_field = layabase.mongo.DictColumn(
            fields={
                "first_key": layabase.mongo.Column(EnumTest, is_nullable=False),
                "second_key": layabase.mongo.Column(int, is_nullable=False),
            },
            is_required=True,
        )

    controller = layabase.CRUDController(TestCollection, history=True)
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

    @namespace.route("/test/history")
    class TestHistoryResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_get_history_parser)
        @namespace.marshal_with(controller.flask_restx.get_history_response_model)
        def get(self):
            return []

    @namespace.route("/test_rollback_parser")
    class TestRollbackParserResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_rollback_parser)
        def get(self):
            return controller.flask_restx.query_rollback_parser.parse_args()

    return application


def test_open_api_definition(client):
    response = client.get("/swagger.json")
    assert response.json == {
        "swagger": "2.0",
        "basePath": "/",
        "paths": {
            "/test": {
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
                            "name": "dict_field.first_key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
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
                            "name": "dict_field.first_key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
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
            },
            "/test/history": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestCollection_GetHistoryResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_history_resource",
                    "parameters": [
                        {
                            "name": "valid_since_revision",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "valid_until_revision",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field.first_key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
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
                }
            },
            "/test_rollback_parser": {
                "get": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "get_test_rollback_parser_resource",
                    "parameters": [
                        {
                            "name": "key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field.first_key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
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
                            "name": "revision",
                            "in": "query",
                            "type": "integer",
                            "minimum": 0,
                            "exclusiveMinimum": True,
                            "required": True,
                        },
                    ],
                    "tags": ["Test"],
                }
            },
        },
        "info": {"title": "API", "version": "1.0"},
        "produces": ["application/json"],
        "consumes": ["application/json"],
        "tags": [{"name": "Test"}],
        "definitions": {
            "TestCollection_PostRequestModel": {
                "required": ["dict_field"],
                "properties": {
                    "dict_field": {
                        "readOnly": False,
                        "default": {"first_key": None, "second_key": None},
                        "example": {"first_key": "Value1", "second_key": 1},
                        "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                },
                "type": "object",
            },
            "first_key_second_key": {
                "properties": {
                    "first_key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
                    },
                    "second_key": {"type": "integer", "readOnly": False, "example": 1},
                },
                "type": "object",
            },
            "TestCollection_PutRequestModel": {
                "required": ["dict_field"],
                "properties": {
                    "dict_field": {
                        "readOnly": False,
                        "default": {"first_key": None, "second_key": None},
                        "example": {"first_key": "Value1", "second_key": 1},
                        "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                },
                "type": "object",
            },
            "TestCollection_GetResponseModel": {
                "required": ["dict_field"],
                "properties": {
                    "dict_field": {
                        "readOnly": False,
                        "default": {"first_key": None, "second_key": None},
                        "example": {"first_key": "Value1", "second_key": 1},
                        "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                },
                "type": "object",
            },
            "TestCollection_GetHistoryResponseModel": {
                "required": ["dict_field"],
                "properties": {
                    "dict_field": {
                        "readOnly": False,
                        "default": {"first_key": None, "second_key": None},
                        "example": {"first_key": "Value1", "second_key": 1},
                        "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "valid_since_revision": {
                        "type": "integer",
                        "description": "Record is valid since this revision (included).",
                        "readOnly": False,
                        "example": 1,
                    },
                    "valid_until_revision": {
                        "type": "integer",
                        "description": "Record is valid until this revision (excluded).",
                        "readOnly": False,
                        "example": 1,
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


def test_query_rollback_parser(client):
    response = client.get(
        "/test_rollback_parser?dict_field.first_key=4&dict_field.second_key=3&key=test&revision=2"
    )
    assert response.json == {
        "dict_field.first_key": ["4"],
        "dict_field.second_key": [3],
        "key": ["test"],
        "revision": 2,
    }
