import enum

import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed

import layabase
import layabase.mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        dict_col = layabase.mongo.DictColumn(
            get_fields=lambda document: {
                "first_key": layabase.mongo.Column(EnumTest, is_nullable=False),
                "second_key": layabase.mongo.Column(int, is_nullable=False),
            }
        )

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


def test_post_dict_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )


def test_get_with_dot_notation_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": EnumTest.Value1, "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == controller.get({"dict_col.first_key": EnumTest.Value1})


def test_get_with_dot_notation_as_list_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": EnumTest.Value1, "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == controller.get({"dict_col.first_key": [EnumTest.Value1]})


def test_get_with_multiple_results_dot_notation_as_list_is_valid(controller):
    controller.post_many(
        [
            {
                "key": "my_key",
                "dict_col": {"first_key": EnumTest.Value1, "second_key": 3},
            },
            {
                "key": "my_key2",
                "dict_col": {"first_key": EnumTest.Value2, "second_key": 4},
            },
        ]
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
        {"dict_col": {"first_key": "Value2", "second_key": 4}, "key": "my_key2"},
    ] == controller.get({"dict_col.first_key": [EnumTest.Value1, EnumTest.Value2]})


def test_update_with_dot_notation_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
        {"dict_col": {"first_key": "Value1", "second_key": 4}, "key": "my_key"},
    ) == controller.put({"key": "my_key", "dict_col.second_key": 4})


def test_update_with_dot_notation_invalid_value_is_invalid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put({"key": "my_key", "dict_col.second_key": "invalid integer"})
    assert {"dict_col.second_key": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "key": "my_key",
        "dict_col.second_key": "invalid integer",
    } == exception_info.value.received_data


def test_delete_with_dot_notation_invalid_value_is_invalid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(ValidationFailed) as exception_info:
        controller.delete({"dict_col.second_key": "invalid integer"})
    assert {"dict_col.second_key": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "dict_col.second_key": "invalid integer"
    } == exception_info.value.received_data


def test_delete_with_dot_notation_valid_value_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == controller.delete({"dict_col.second_key": 3})


def test_delete_with_dot_notation_enum_value_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == controller.delete({"dict_col.first_key": EnumTest.Value1})


def test_post_with_dot_notation_invalid_value_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post(
            {
                "key": "my_key",
                "dict_col.first_key": "Value1",
                "dict_col.second_key": "invalid integer",
            }
        )
    assert {"dict_col.second_key": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "key": "my_key",
        "dict_col.first_key": "Value1",
        "dict_col.second_key": "invalid integer",
    } == exception_info.value.received_data


def test_post_with_dot_notation_valid_value_is_valid(controller):
    assert {
        "key": "my_key",
        "dict_col": {"first_key": "Value2", "second_key": 1},
    } == controller.post(
        {"key": "my_key", "dict_col.first_key": "Value2", "dict_col.second_key": 1}
    )


def test_get_with_unmatching_dot_notation_is_empty(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert [] == controller.get({"dict_col.first_key": "Value2"})


def test_get_with_unknown_dot_notation_returns_everything(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == controller.get({"dict_col.unknown": "Value1"})


def test_delete_with_dot_notation_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == controller.delete({"dict_col.first_key": "Value1"})
    assert [] == controller.get({})


def test_delete_with_unmatching_dot_notation_is_empty(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 0 == controller.delete({"dict_col.first_key": "Value2"})
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == controller.get({})


def test_delete_with_unknown_dot_notation_deletes_everything(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == controller.delete({"dict_col.unknown": "Value2"})
    assert [] == controller.get({})


def test_put_without_primary_key_is_invalid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put({"dict_col": {"first_key": "Value2", "second_key": 4}})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {
        "dict_col": {"first_key": "Value2", "second_key": 4}
    } == exception_info.value.received_data


def test_post_dict_with_dot_notation_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col.first_key": "Value1", "dict_col.second_key": 3}
    )


def test_put_dict_with_dot_notation_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
        {"dict_col": {"first_key": "Value2", "second_key": 3}, "key": "my_key"},
    ) == controller.put({"key": "my_key", "dict_col.first_key": EnumTest.Value2})


def test_post_dict_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"key": "my_key", "dict_col": {"first_key": "Value1"}})
    assert {
        "dict_col.second_key": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {
        "key": "my_key",
        "dict_col": {"first_key": "Value1"},
    } == exception_info.value.received_data


def test_open_api_definition(client):
    response = client.get("/swagger.json")
    assert response.json == {
        "basePath": "/",
        "consumes": ["application/json"],
        "definitions": {
            "TestCollection_GetResponseModel": {
                "properties": {
                    "dict_col": {"default": {}, "readOnly": False, "type": "object"},
                    "key": {
                        "example": "sample " "key",
                        "readOnly": False,
                        "type": "string",
                    },
                },
                "type": "object",
            },
            "TestCollection_PostRequestModel": {
                "properties": {
                    "dict_col": {"default": {}, "readOnly": False, "type": "object"},
                    "key": {
                        "example": "sample " "key",
                        "readOnly": False,
                        "type": "string",
                    },
                },
                "type": "object",
            },
            "TestCollection_PutRequestModel": {
                "properties": {
                    "dict_col": {"default": {}, "readOnly": False, "type": "object"},
                    "key": {
                        "example": "sample " "key",
                        "readOnly": False,
                        "type": "string",
                    },
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
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        }
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
                            "name": "key",
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
                                "$ref": "#/definitions/TestCollection_GetResponseModel"
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
                                "$ref": "#/definitions/TestCollection_PostRequestModel"
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
                                "$ref": "#/definitions/TestCollection_PutRequestModel"
                            },
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
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        }
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
                            "items": {"type": "string"},
                            "name": "key",
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


def test_query_get_parser_with_dict(client):
    response = client.get(
        "/test_parsers?dict_col.first_key=2&dict_col.second_key=3&key=4&limit=1&offset=0"
    )
    assert response.json == {"key": ["4"], "limit": 1, "offset": 0}


def test_query_delete_parser_with_dict(client):
    response = client.delete(
        "/test_parsers?dict_col.first_key=2&dict_col.second_key=3&key=4"
    )
    assert response.json == {"key": ["4"]}
