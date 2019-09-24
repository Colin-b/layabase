import enum
import json

import flask
import flask_restplus
import pytest

from layabase import database, database_mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestListController(database.CRUDController):
    pass


def _create_models(base):
    class TestListModel(
        database_mongo.CRUDModel, base=base, table_name="list_table_name"
    ):
        key = database_mongo.Column(is_primary_key=True)
        list_field = database_mongo.ListColumn(
            database_mongo.DictColumn(
                fields={
                    "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                    "second_key": database_mongo.Column(int, is_nullable=False),
                }
            )
        )
        bool_field = database_mongo.Column(bool)

    TestListController.model(TestListModel)

    return [TestListModel]


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

    TestListController.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(TestListController.query_get_parser)
        @namespace.marshal_with(TestListController.get_response_model)
        def get(self):
            return []

        @namespace.expect(TestListController.json_post_model)
        def post(self):
            return []

        @namespace.expect(TestListController.json_put_model)
        def put(self):
            return []

        @namespace.expect(TestListController.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        @namespace.expect(TestListController.query_get_parser)
        def get(self):
            return TestListController.query_get_parser.parse_args()

        @namespace.expect(TestListController.query_delete_parser)
        def delete(self):
            return TestListController.query_delete_parser.parse_args()

    return application


def test_open_api_definition(client):
    response = client.get("/swagger.json")
    assert response.json == {
        "basePath": "/",
        "consumes": ["application/json"],
        "definitions": {
            "TestListModel": {
                "properties": {
                    "bool_field": {
                        "example": True,
                        "readOnly": False,
                        "type": "boolean",
                    },
                    "key": {
                        "example": "sample " "key",
                        "readOnly": False,
                        "type": "string",
                    },
                    "list_field": {
                        "example": [{"first_key": "Value1", "second_key": 1}],
                        "items": {
                            "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                            "default": {"first_key": None, "second_key": None},
                            "example": {"first_key": "Value1", "second_key": 1},
                            "readOnly": False,
                        },
                        "readOnly": False,
                        "type": "array",
                    },
                },
                "type": "object",
            },
            "first_key_second_key": {
                "properties": {
                    "first_key": {
                        "enum": ["Value1", "Value2"],
                        "example": "Value1",
                        "readOnly": False,
                        "type": "string",
                    },
                    "second_key": {"example": 1, "readOnly": False, "type": "integer"},
                },
                "type": "object",
            },
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
                            "items": {"type": "boolean"},
                            "name": "bool_field",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "list_field",
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
                            "items": {"type": "boolean"},
                            "name": "bool_field",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "list_field",
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
                            "schema": {"$ref": "#/definitions/TestListModel"},
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
                            "schema": {"$ref": "#/definitions/TestListModel"},
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
                            "schema": {"$ref": "#/definitions/TestListModel"},
                        }
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                },
            },
            "/test_parsers": {
                "delete": {
                    "operationId": "delete_test_parsers_resource",
                    "parameters": [
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "boolean"},
                            "name": "bool_field",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "list_field",
                            "type": "array",
                        },
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                },
                "get": {
                    "operationId": "get_test_parsers_resource",
                    "parameters": [
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "boolean"},
                            "name": "bool_field",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "list_field",
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
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                },
            },
        },
        "produces": ["application/json"],
        "responses": {
            "MaskError": {"description": "When any error occurs on mask"},
            "ParseError": {"description": "When a mask can't be parsed"},
        },
        "swagger": "2.0",
        "tags": [{"name": "Test"}],
    }


def test_post_list_of_dict_is_valid(db):
    assert {
        "bool_field": False,
        "key": "my_key",
        "list_field": [
            {"first_key": "Value1", "second_key": 1},
            {"first_key": "Value2", "second_key": 2},
        ],
    } == TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )


def test_post_optional_missing_list_of_dict_is_valid(db):
    assert {
        "bool_field": False,
        "key": "my_key",
        "list_field": None,
    } == TestListController.post({"key": "my_key", "bool_field": False})


def test_post_optional_list_of_dict_as_none_is_valid(db):
    assert {
        "bool_field": False,
        "key": "my_key",
        "list_field": None,
    } == TestListController.post(
        {"key": "my_key", "bool_field": False, "list_field": None}
    )


def test_get_list_of_dict_is_valid(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert [
        {
            "bool_field": False,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value1", "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ],
        }
    ] == TestListController.get(
        {
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ]
        }
    )


def test_get_optional_list_of_dict_as_None_is_skipped(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert [
        {
            "bool_field": False,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value1", "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ],
        }
    ] == TestListController.get({"list_field": None})


def test_delete_list_of_dict_is_valid(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert 1 == TestListController.delete(
        {
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ]
        }
    )


def test_delete_optional_list_of_dict_as_None_is_valid(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert 1 == TestListController.delete({"list_field": None})


def test_put_list_of_dict_is_valid(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert (
        {
            "bool_field": False,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value1", "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ],
        },
        {
            "bool_field": True,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value2", "second_key": 10},
                {"first_key": "Value1", "second_key": 2},
            ],
        },
    ) == TestListController.put(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value2, "second_key": 10},
                {"first_key": EnumTest.Value1, "second_key": 2},
            ],
            "bool_field": True,
        }
    )


def test_put_without_optional_list_of_dict_is_valid(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert (
        {
            "bool_field": False,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value1", "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ],
        },
        {
            "bool_field": True,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value1", "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ],
        },
    ) == TestListController.put({"key": "my_key", "bool_field": True})


def test_query_get_parser_with_list_of_dict(client):
    response = client.get(
        "/test_parsers?bool_field=true&key=test&list_field=[1,2]&limit=1&offset=0"
    )
    assert response.json == {
        "bool_field": [True],
        "key": ["test"],
        "limit": 1,
        "list_field": [[1, 2]],
        "offset": 0,
    }


def test_query_delete_parser_with_list_of_dict(client):
    response = client.delete("/test_parsers?bool_field=true&key=test&list_field=[1,2]")
    assert response.json == {
        "bool_field": [True],
        "key": ["test"],
        "list_field": [[1, 2]],
    }
