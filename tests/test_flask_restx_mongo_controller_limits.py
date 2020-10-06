import flask
import flask_restx
import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(is_primary_key=True, min_length=3, max_length=4)
        list_field = layabase.mongo.Column(
            list, min_length=2, max_length=3, example=["my", "test"]
        )
        dict_field = layabase.mongo.Column(
            dict, min_length=2, max_length=3, example={"my": 1, "test": 2}
        )
        int_field = layabase.mongo.Column(int, min_value=100, max_value=999)
        float_field = layabase.mongo.Column(float, min_value=1.25, max_value=1.75)

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
                            "name": "list_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "int_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "float_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "number"},
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
                            "name": "list_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "int_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "float_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "number"},
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
            }
        },
        "info": {"title": "API", "version": "1.0"},
        "produces": ["application/json"],
        "consumes": ["application/json"],
        "tags": [{"name": "Test"}],
        "definitions": {
            "TestCollection_PostRequestModel": {
                "properties": {
                    "dict_field": {
                        "type": "object",
                        "readOnly": False,
                        "example": {"my": 1, "test": 2},
                    },
                    "float_field": {
                        "type": "number",
                        "readOnly": False,
                        "example": 1.4,
                        "minimum": 1.25,
                        "maximum": 1.75,
                    },
                    "int_field": {
                        "type": "integer",
                        "readOnly": False,
                        "example": 100,
                        "minimum": 100,
                        "maximum": 999,
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "XXX",
                        "minLength": 3,
                        "maxLength": 4,
                    },
                    "list_field": {
                        "type": "array",
                        "readOnly": False,
                        "example": ["my", "test"],
                        "minItems": 2,
                        "maxItems": 3,
                        "items": {"type": "string"},
                    },
                },
                "type": "object",
            },
            "TestCollection_PutRequestModel": {
                "properties": {
                    "dict_field": {
                        "type": "object",
                        "readOnly": False,
                        "example": {"my": 1, "test": 2},
                    },
                    "float_field": {
                        "type": "number",
                        "readOnly": False,
                        "example": 1.4,
                        "minimum": 1.25,
                        "maximum": 1.75,
                    },
                    "int_field": {
                        "type": "integer",
                        "readOnly": False,
                        "example": 100,
                        "minimum": 100,
                        "maximum": 999,
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "XXX",
                        "minLength": 3,
                        "maxLength": 4,
                    },
                    "list_field": {
                        "type": "array",
                        "readOnly": False,
                        "example": ["my", "test"],
                        "minItems": 2,
                        "maxItems": 3,
                        "items": {"type": "string"},
                    },
                },
                "type": "object",
            },
            "TestCollection_GetResponseModel": {
                "properties": {
                    "dict_field": {
                        "type": "object",
                        "readOnly": False,
                        "example": {"my": 1, "test": 2},
                    },
                    "float_field": {
                        "type": "number",
                        "readOnly": False,
                        "example": 1.4,
                        "minimum": 1.25,
                        "maximum": 1.75,
                    },
                    "int_field": {
                        "type": "integer",
                        "readOnly": False,
                        "example": 100,
                        "minimum": 100,
                        "maximum": 999,
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "XXX",
                        "minLength": 3,
                        "maxLength": 4,
                    },
                    "list_field": {
                        "type": "array",
                        "readOnly": False,
                        "example": ["my", "test"],
                        "minItems": 2,
                        "maxItems": 3,
                        "items": {"type": "string"},
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
