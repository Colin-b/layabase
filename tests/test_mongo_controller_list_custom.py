import flask
import flask_restplus
import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(is_primary_key=True)
        list_field = layabase.mongo.Column(list)

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


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
                "get": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "get_test_parsers_resource",
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
                "delete": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "delete_test_parsers_resource",
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
            "TestCollection_PostRequestModel": {
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
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
            "TestCollection_PutRequestModel": {
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
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
            "TestCollection_GetResponseModel": {
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
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


def test_post_list_of_dict_is_valid(controller):
    assert controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    ) == {
        "key": "my_key",
        "list_field": [
            {"first_key": "key1", "second_key": 1},
            {"first_key": "key2", "second_key": 2},
        ],
    }


def test_post_optional_missing_list_of_dict_is_valid(controller):
    assert controller.post({"key": "my_key"}) == {"key": "my_key", "list_field": None}


def test_post_optional_list_of_dict_as_none_is_valid(controller):
    assert controller.post({"key": "my_key", "list_field": None}) == {
        "key": "my_key",
        "list_field": None,
    }


def test_get_list_of_dict_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert controller.get(
        {
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ]
        }
    ) == [
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    ]


def test_get_optional_list_of_dict_as_none_is_skipped(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert controller.get({"list_field": None}) == [
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    ]


def test_delete_list_of_dict_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert (
        controller.delete(
            {
                "list_field": [
                    {"first_key": "key1", "second_key": 1},
                    {"first_key": "key2", "second_key": 2},
                ]
            }
        )
        == 1
    )


def test_delete_optional_list_of_dict_as_none_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert controller.delete({"list_field": None}) == 1


def test_put_list_of_dict_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert controller.put(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key2", "second_key": 10},
                {"first_key": "key1", "second_key": 2},
            ],
        }
    ) == (
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        },
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key2", "second_key": 10},
                {"first_key": "key1", "second_key": 2},
            ],
        },
    )


def test_put_without_optional_list_of_dict_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert controller.put({"key": "my_key"}) == (
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        },
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        },
    )


def test_query_get_parser_with_list(client):
    response = client.get("/test_parsers?key=test&list_field=[1,2]&limit=1&offset=0")
    assert response.json == {
        "key": ["test"],
        "limit": 1,
        "list_field": [[1, 2]],
        "offset": 0,
    }


def test_query_delete_parser_with_list(client):
    response = client.delete("/test_parsers?key=test&list_field=[1,2]")
    assert response.json == {"key": ["test"], "list_field": [[1, 2]]}
