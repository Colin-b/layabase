import enum

import flask
import flask_restplus
import pytest

import layabase
import layabase.database_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller():
    class TestController(layabase.CRUDController):
        class TestListModel:
            __tablename__ = "list_table_name"

            key = layabase.database_mongo.Column(is_primary_key=True)
            list_field = layabase.database_mongo.ListColumn(
                layabase.database_mongo.DictColumn(
                    fields={
                        "first_key": layabase.database_mongo.Column(
                            EnumTest, is_nullable=False
                        ),
                        "second_key": layabase.database_mongo.Column(
                            int, is_nullable=False
                        ),
                    }
                )
            )
            bool_field = layabase.database_mongo.Column(bool)

        model = TestListModel

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

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        @namespace.expect(controller.query_get_parser)
        def get(self):
            return controller.query_get_parser.parse_args()

        @namespace.expect(controller.query_delete_parser)
        def delete(self):
            return controller.query_delete_parser.parse_args()

    return application


def test_open_api_definition(client):
    response = client.get("/swagger.json")
    assert response.json == {
        "swagger": "2.0",
        "basePath": "/",
        "paths": {
            "/test": {
                "delete": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "delete_test_resource",
                    "parameters": [
                        {
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
                "post": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "post_test_resource",
                    "parameters": [
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": {
                                "$ref": "#/definitions/TestListModel_PostRequestModel"
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
                                "$ref": "#/definitions/TestListModel_PutRequestModel"
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
                                "$ref": "#/definitions/TestListModel_GetResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_resource",
                    "parameters": [
                        {
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
            },
            "/test_parsers": {
                "delete": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "delete_test_parsers_resource",
                    "parameters": [
                        {
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
                "get": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "get_test_parsers_resource",
                    "parameters": [
                        {
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
                    ],
                    "tags": ["Test"],
                },
            },
        },
        "info": {"title": "API", "version": "1.0"},
        "produces": ["application/json"],
        "consumes": ["application/json"],
        "tags": [{"name": "Test"}],
        "definitions": {
            "TestListModel_PostRequestModel": {
                "properties": {
                    "bool_field": {
                        "type": "boolean",
                        "readOnly": False,
                        "example": True,
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "list_field": {
                        "type": "array",
                        "readOnly": False,
                        "example": [{"first_key": "Value1", "second_key": 1}],
                        "items": {
                            "readOnly": False,
                            "default": {"first_key": None, "second_key": None},
                            "example": {"first_key": "Value1", "second_key": 1},
                            "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                        },
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
            "TestListModel_PutRequestModel": {
                "properties": {
                    "bool_field": {
                        "type": "boolean",
                        "readOnly": False,
                        "example": True,
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "list_field": {
                        "type": "array",
                        "readOnly": False,
                        "example": [{"first_key": "Value1", "second_key": 1}],
                        "items": {
                            "readOnly": False,
                            "default": {"first_key": None, "second_key": None},
                            "example": {"first_key": "Value1", "second_key": 1},
                            "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                        },
                    },
                },
                "type": "object",
            },
            "TestListModel_GetResponseModel": {
                "properties": {
                    "bool_field": {
                        "type": "boolean",
                        "readOnly": False,
                        "example": True,
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "list_field": {
                        "type": "array",
                        "readOnly": False,
                        "example": [{"first_key": "Value1", "second_key": 1}],
                        "items": {
                            "readOnly": False,
                            "default": {"first_key": None, "second_key": None},
                            "example": {"first_key": "Value1", "second_key": 1},
                            "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                        },
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


def test_post_list_of_dict_is_valid(controller):
    assert {
        "bool_field": False,
        "key": "my_key",
        "list_field": [
            {"first_key": "Value1", "second_key": 1},
            {"first_key": "Value2", "second_key": 2},
        ],
    } == controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )


def test_post_optional_missing_list_of_dict_is_valid(controller):
    assert {
        "bool_field": False,
        "key": "my_key",
        "list_field": None,
    } == controller.post({"key": "my_key", "bool_field": False})


def test_post_optional_list_of_dict_as_none_is_valid(controller):
    assert {
        "bool_field": False,
        "key": "my_key",
        "list_field": None,
    } == controller.post({"key": "my_key", "bool_field": False, "list_field": None})


def test_get_list_of_dict_is_valid(controller):
    controller.post(
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
    ] == controller.get(
        {
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ]
        }
    )


def test_get_optional_list_of_dict_as_none_is_skipped(controller):
    controller.post(
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
    ] == controller.get({"list_field": None})


def test_delete_list_of_dict_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert 1 == controller.delete(
        {
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ]
        }
    )


def test_delete_optional_list_of_dict_as_none_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert 1 == controller.delete({"list_field": None})


def test_put_list_of_dict_is_valid(controller):
    controller.post(
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
    ) == controller.put(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value2, "second_key": 10},
                {"first_key": EnumTest.Value1, "second_key": 2},
            ],
            "bool_field": True,
        }
    )


def test_put_without_optional_list_of_dict_is_valid(controller):
    controller.post(
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
    ) == controller.put({"key": "my_key", "bool_field": True})


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
