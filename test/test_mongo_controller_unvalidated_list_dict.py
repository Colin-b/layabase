import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo
import layabase.testing


class TestUnvalidatedListAndDictController(database.CRUDController):
    class TestUnvalidatedListAndDictModel:
        __tablename__ = "list_and_dict_table_name"

        float_key = database_mongo.Column(float, is_primary_key=True)
        float_with_default = database_mongo.Column(float, default_value=34)
        dict_field = database_mongo.Column(dict, is_required=True)
        list_field = database_mongo.Column(list, is_required=True)

    model = TestUnvalidatedListAndDictModel


@pytest.fixture
def db():
    _db = database.load("mongomock", [TestUnvalidatedListAndDictController])
    yield _db
    layabase.testing.reset(_db)


@pytest.fixture
def app(db):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    TestUnvalidatedListAndDictController.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(TestUnvalidatedListAndDictController.query_get_parser)
        @namespace.marshal_with(TestUnvalidatedListAndDictController.get_response_model)
        def get(self):
            return []

        @namespace.expect(TestUnvalidatedListAndDictController.json_post_model)
        def post(self):
            return []

        @namespace.expect(TestUnvalidatedListAndDictController.json_put_model)
        def put(self):
            return []

        @namespace.expect(TestUnvalidatedListAndDictController.query_delete_parser)
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
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestUnvalidatedListAndDictModel_GetResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_resource",
                    "parameters": [
                        {
                            "name": "dict_field",
                            "in": "query",
                            "type": "array",
                            "required": True,
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "float_key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "number"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "float_with_default",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "number"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "list_field",
                            "in": "query",
                            "type": "array",
                            "required": True,
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
                "post": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "post_test_resource",
                    "parameters": [
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": {
                                "$ref": "#/definitions/TestUnvalidatedListAndDictModel_PostRequestModel"
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
                                "$ref": "#/definitions/TestUnvalidatedListAndDictModel_PutRequestModel"
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
                            "name": "dict_field",
                            "in": "query",
                            "type": "array",
                            "required": True,
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "float_key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "number"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "float_with_default",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "number"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "list_field",
                            "in": "query",
                            "type": "array",
                            "required": True,
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
            "TestUnvalidatedListAndDictModel_PostRequestModel": {
                "required": ["dict_field", "list_field"],
                "properties": {
                    "dict_field": {
                        "type": "object",
                        "readOnly": False,
                        "example": {
                            "1st dict_field key": "1st dict_field sample",
                            "2nd dict_field key": "2nd dict_field sample",
                        },
                    },
                    "float_key": {"type": "number", "readOnly": False, "example": 1.4},
                    "float_with_default": {
                        "type": "number",
                        "readOnly": False,
                        "default": 34,
                        "example": 34,
                    },
                    "list_field": {
                        "type": "array",
                        "readOnly": False,
                        "example": ["1st list_field sample", "2nd list_field sample"],
                        "items": {"type": "string"},
                    },
                },
                "type": "object",
            },
            "TestUnvalidatedListAndDictModel_PutRequestModel": {
                "required": ["dict_field", "list_field"],
                "properties": {
                    "dict_field": {
                        "type": "object",
                        "readOnly": False,
                        "example": {
                            "1st dict_field key": "1st dict_field sample",
                            "2nd dict_field key": "2nd dict_field sample",
                        },
                    },
                    "float_key": {"type": "number", "readOnly": False, "example": 1.4},
                    "float_with_default": {
                        "type": "number",
                        "readOnly": False,
                        "default": 34,
                        "example": 34,
                    },
                    "list_field": {
                        "type": "array",
                        "readOnly": False,
                        "example": ["1st list_field sample", "2nd list_field sample"],
                        "items": {"type": "string"},
                    },
                },
                "type": "object",
            },
            "TestUnvalidatedListAndDictModel_GetResponseModel": {
                "required": ["dict_field", "list_field"],
                "properties": {
                    "dict_field": {
                        "type": "object",
                        "readOnly": False,
                        "example": {
                            "1st dict_field key": "1st dict_field sample",
                            "2nd dict_field key": "2nd dict_field sample",
                        },
                    },
                    "float_key": {"type": "number", "readOnly": False, "example": 1.4},
                    "float_with_default": {
                        "type": "number",
                        "readOnly": False,
                        "default": 34,
                        "example": 34,
                    },
                    "list_field": {
                        "type": "array",
                        "readOnly": False,
                        "example": ["1st list_field sample", "2nd list_field sample"],
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


def test_post_float_as_int(db):
    assert {
        "dict_field": {"any_key": 5},
        "float_key": 1,
        "float_with_default": 34,
        "list_field": [22, "33", 44.55, True],
    } == TestUnvalidatedListAndDictController.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    )


def test_get_float_as_int(db):
    TestUnvalidatedListAndDictController.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    )
    assert {
        "dict_field": {"any_key": 5},
        "float_key": 1,
        "float_with_default": 34,
        "list_field": [22, "33", 44.55, True],
    } == TestUnvalidatedListAndDictController.get_one({"float_key": 1})


def test_put_float_as_int(db):
    TestUnvalidatedListAndDictController.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    )
    assert (
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "float_with_default": 34,
            "list_field": [22, "33", 44.55, True],
        },
        {
            "dict_field": {"any_key": 6},
            "float_key": 1,
            "float_with_default": 35,
            "list_field": [22, "33", 44.55, True],
        },
    ) == TestUnvalidatedListAndDictController.put(
        {"dict_field.any_key": 6, "float_key": 1, "float_with_default": 35}
    )


def test_get_with_required_field_as_none_is_invalid(db):
    TestUnvalidatedListAndDictController.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    )
    with pytest.raises(ValidationFailed) as exception_info:
        TestUnvalidatedListAndDictController.get({"dict_field": None})
    assert exception_info.value.errors == {
        "dict_field": ["Missing data for required field."]
    }
    assert {"dict_field": None} == exception_info.value.received_data
