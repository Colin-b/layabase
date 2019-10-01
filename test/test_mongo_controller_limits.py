import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed

import layabase
import layabase.database_mongo
import layabase.testing


@pytest.fixture
def controller():
    class TestModel:
        __tablename__ = "test"

        key = layabase.database_mongo.Column(
            is_primary_key=True, min_length=3, max_length=4
        )
        list_field = layabase.database_mongo.Column(
            list, min_length=2, max_length=3, example=["my", "test"]
        )
        dict_field = layabase.database_mongo.Column(
            dict, min_length=2, max_length=3, example={"my": 1, "test": 2}
        )
        int_field = layabase.database_mongo.Column(int, min_value=100, max_value=999)
        float_field = layabase.database_mongo.Column(
            float, min_value=1.25, max_value=1.75
        )

    controller = layabase.CRUDController(TestModel)
    _db = layabase.load("mongomock", [controller])
    yield controller
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


def test_within_limits_is_valid(controller):
    assert {
        "dict_field": {"my": 1, "test": 2},
        "int_field": 100,
        "float_field": 1.3,
        "key": "111",
        "list_field": ["1", "2", "3"],
    } == controller.post(
        {
            "dict_field": {"my": 1, "test": 2},
            "key": "111",
            "list_field": ["1", "2", "3"],
            "int_field": 100,
            "float_field": 1.3,
        }
    )


def test_outside_upper_limits_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post(
            {
                "key": "11111",
                "list_field": ["1", "2", "3", "4", "5"],
                "int_field": 1000,
                "float_field": 1.1,
                "dict_field": {"my": 1, "test": 2, "is": 3, "invalid": 4},
            }
        )
    assert {
        "int_field": ['Value "1000" is too big. Maximum value is 999.'],
        "key": ['Value "11111" is too big. Maximum length is 4.'],
        "float_field": ['Value "1.1" is too small. Minimum value is 1.25.'],
        "list_field": [
            "['1', '2', '3', '4', '5'] contains too many values. Maximum length is 3."
        ],
        "dict_field": [
            "{'my': 1, 'test': 2, 'is': 3, 'invalid': 4} contains too many values. Maximum length is 3."
        ],
    } == exception_info.value.errors
    assert {
        "int_field": 1000,
        "float_field": 1.1,
        "key": "11111",
        "list_field": ["1", "2", "3", "4", "5"],
        "dict_field": {"my": 1, "test": 2, "is": 3, "invalid": 4},
    } == exception_info.value.received_data


def test_outside_lower_limits_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post(
            {
                "key": "11",
                "list_field": ["1"],
                "int_field": 99,
                "dict_field": {"my": 1},
                "float_field": 2.1,
            }
        )
    assert {
        "dict_field": [
            "{'my': 1} does not contains enough values. Minimum length is 2."
        ],
        "int_field": ['Value "99" is too small. Minimum value is 100.'],
        "float_field": ['Value "2.1" is too big. Maximum value is 1.75.'],
        "key": ['Value "11" is too small. Minimum length is 3.'],
        "list_field": ["['1'] does not contains enough values. Minimum length is 2."],
    } == exception_info.value.errors
    assert {
        "key": "11",
        "list_field": ["1"],
        "int_field": 99,
        "dict_field": {"my": 1},
        "float_field": 2.1,
    } == exception_info.value.received_data


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
                                "$ref": "#/definitions/TestModel_PostRequestModel"
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
                                "$ref": "#/definitions/TestModel_PutRequestModel"
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
                                "$ref": "#/definitions/TestModel_GetResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_resource",
                    "parameters": [
                        {
                            "name": "dict_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
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
                            "name": "int_field",
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
                            "name": "list_field",
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
                            "name": "dict_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
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
                            "name": "int_field",
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
                            "name": "list_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
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
            "TestModel_PostRequestModel": {
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
            "TestModel_PutRequestModel": {
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
            "TestModel_GetResponseModel": {
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
