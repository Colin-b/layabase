import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo


class TestUnvalidatedListAndDictController(database.CRUDController):
    pass


def _create_models(base):
    class TestUnvalidatedListAndDictModel(
        database_mongo.CRUDModel, base=base, table_name="list_and_dict_table_name"
    ):
        float_key = database_mongo.Column(float, is_primary_key=True)
        float_with_default = database_mongo.Column(float, default_value=34)
        dict_field = database_mongo.Column(dict, is_required=True)
        list_field = database_mongo.Column(list, is_required=True)

    TestUnvalidatedListAndDictController.model(TestUnvalidatedListAndDictModel)

    return [TestUnvalidatedListAndDictModel]


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
        "basePath": "/",
        "consumes": ["application/json"],
        "definitions": {
            "TestUnvalidatedListAndDictModel": {
                "properties": {
                    "dict_field": {
                        "example": {
                            "1st dict_field key": "1st " "dict_field " "sample",
                            "2nd dict_field key": "2nd " "dict_field " "sample",
                        },
                        "readOnly": False,
                        "type": "object",
                    },
                    "float_key": {"example": 1.4, "readOnly": False, "type": "number"},
                    "float_with_default": {
                        "default": 34,
                        "example": 34,
                        "readOnly": False,
                        "type": "number",
                    },
                    "list_field": {
                        "example": [
                            "1st " "list_field " "sample",
                            "2nd " "list_field " "sample",
                        ],
                        "items": {"type": "string"},
                        "readOnly": False,
                        "type": "array",
                    },
                },
                "required": ["dict_field", "list_field"],
                "type": "object",
            }
        },
        "info": {"title": "API", "version": "1.0"},
        "paths": {
            "/test": {
                "delete": {
                    "operationId": "delete_test_resource",
                    "parameters": [
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "dict_field",
                            "required": True,
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "float_key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "float_with_default",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "list_field",
                            "required": True,
                            "type": "array",
                        },
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                },
                "get": {
                    "operationId": "get_test_resource",
                    "parameters": [
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "dict_field",
                            "required": True,
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "float_key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "float_with_default",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "list_field",
                            "required": True,
                            "type": "array",
                        },
                        {
                            "exclusiveMinimum": True,
                            "in": "query",
                            "minimum": 0,
                            "name": "limit",
                            "type": "integer",
                        },
                        {
                            "in": "query",
                            "minimum": 0,
                            "name": "offset",
                            "type": "integer",
                        },
                        {
                            "description": "An optional " "fields mask",
                            "format": "mask",
                            "in": "header",
                            "name": "X-Fields",
                            "type": "string",
                        },
                    ],
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestUnvalidatedListAndDictModel"
                            },
                        }
                    },
                    "tags": ["Test"],
                },
                "post": {
                    "operationId": "post_test_resource",
                    "parameters": [
                        {
                            "in": "body",
                            "name": "payload",
                            "required": True,
                            "schema": {
                                "$ref": "#/definitions/TestUnvalidatedListAndDictModel"
                            },
                        }
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                },
                "put": {
                    "operationId": "put_test_resource",
                    "parameters": [
                        {
                            "in": "body",
                            "name": "payload",
                            "required": True,
                            "schema": {
                                "$ref": "#/definitions/TestUnvalidatedListAndDictModel"
                            },
                        }
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                },
            }
        },
        "produces": ["application/json"],
        "responses": {
            "MaskError": {"description": "When any error occurs on mask"},
            "ParseError": {"description": "When a mask can't be parsed"},
        },
        "swagger": "2.0",
        "tags": [{"name": "Test"}],
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
