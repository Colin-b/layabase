import pytest
import sqlalchemy
import flask
import flask_restx

import layabase


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        test_field = sqlalchemy.Column(sqlalchemy.Enum("chose1", "chose2"))

    controller = layabase.CRUDController(TestTable)
    layabase.load("sqlite:///:memory:", [controller])
    return controller


@pytest.fixture
def app(controller: layabase.CRUDController):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restx.Api(application)
    namespace = api.namespace("Test", path="/", validate=True)

    controller.flask_restx.init_models(namespace)

    @namespace.route("/test")
    class TestResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_get_parser)
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

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restx.Resource):
        def get(self):
            return controller.flask_restx.query_get_parser.parse_args()

        def delete(self):
            return controller.flask_restx.query_delete_parser.parse_args()

    return application


def test_query_get_parser_without_value_for_field(client):
    response = client.get("/test_parsers")
    assert response.status_code == 200
    assert response.json == {
        "key": None,
        "test_field": None,
        "limit": None,
        "offset": None,
        "order_by": None,
    }


def test_query_get_parser_with_invalid_value_for_field(client):
    response = client.get("/test_parsers?test_field=chose3")
    assert response.status_code == 400
    assert response.json == {
        "errors": {
            "test_field": "The value 'chose3' is not a valid choice for 'test_field'."
        },
        "message": "Input payload validation failed",
    }


def test_query_get_parser_with_valid_and_invalid_value_for_field(client):
    response = client.get("/test_parsers?test_field=chose1&test_field=chose3")
    assert response.status_code == 400
    assert response.json == {
        "errors": {
            "test_field": "The value 'chose3' is not a valid choice for 'test_field'."
        },
        "message": "Input payload validation failed",
    }


def test_query_get_parser_with_valid_value_for_field(client):
    response = client.get("/test_parsers?test_field=chose1")
    assert response.status_code == 200
    assert response.json == {
        "key": None,
        "test_field": ["chose1"],
        "limit": None,
        "offset": None,
        "order_by": None,
    }


def test_query_get_parser_with_valid_values_for_field(client):
    response = client.get("/test_parsers?test_field=chose1&test_field=chose2")
    assert response.status_code == 200
    assert response.json == {
        "key": None,
        "limit": None,
        "offset": None,
        "order_by": None,
        "test_field": ["chose1", "chose2"],
    }


def test_query_delete_parser_without_value_for_field(client):
    response = client.delete("/test_parsers")
    assert response.status_code == 200
    assert response.json == {"key": None, "test_field": None}


def test_query_delete_parser_with_invalid_value_for_field(client):
    response = client.delete("/test_parsers?test_field=chose3")
    assert response.status_code == 400
    assert response.json == {
        "errors": {
            "test_field": "The value 'chose3' is not a valid choice for 'test_field'."
        },
        "message": "Input payload validation failed",
    }


def test_query_delete_parser_with_valid_and_invalid_value_for_field(client):
    response = client.delete("/test_parsers?test_field=chose1&test_field=chose3")
    assert response.status_code == 400
    assert response.json == {
        "errors": {
            "test_field": "The value 'chose3' is not a valid choice for 'test_field'."
        },
        "message": "Input payload validation failed",
    }


def test_query_delete_parser_with_valid_value_for_field(client):
    response = client.delete("/test_parsers?test_field=chose1")
    assert response.status_code == 200
    assert response.json == {"key": None, "test_field": ["chose1"]}


def test_query_delete_parser_with_valid_values_for_field(client):
    response = client.delete("/test_parsers?test_field=chose1&test_field=chose2")
    assert response.status_code == 200
    assert response.json == {"key": None, "test_field": ["chose1", "chose2"]}


def test_post_request_with_valid_value_for_field(client):
    response = client.post("/test", json={"key": "1", "test_field": "chose2"})
    assert response.status_code == 200


def test_post_request_with_invalid_value_for_field(client):
    response = client.post("/test", json={"key": "1", "test_field": "chose_invalid"})
    assert response.status_code == 400
    assert response.json == {
        "errors": {"test_field": "'chose_invalid' is not one of ['chose1', 'chose2']"},
        "message": "Input payload validation failed",
    }


def test_post_request_with_empty_value_for_field(client):
    response = client.post("/test", json={"key": "1", "test_field": ""})
    assert response.status_code == 400
    assert response.json == {
        "errors": {"test_field": "'' is not one of ['chose1', 'chose2']"},
        "message": "Input payload validation failed",
    }


def test_put_request_with_valid_value_for_field(client):
    client.post("/test", json={"key": "0", "test_field": "chose1"})
    response = client.put("/test", json={"key": "1", "test_field": "chose2"})
    assert response.status_code == 200


def test_put_request_with_invalid_value_for_field(client):
    client.post("/test", json={"key": "0", "test_field": "chose1"})
    response = client.put("/test", json={"key": "1", "test_field": "chose_invalid"})
    assert response.status_code == 400
    assert response.json == {
        "errors": {"test_field": "'chose_invalid' is not one of ['chose1', 'chose2']"},
        "message": "Input payload validation failed",
    }


def test_put_request_with_empty_value_for_field(client):
    client.post("/test", json={"key": "0", "test_field": "chose1"})
    response = client.put("/test", json={"key": "1", "test_field": ""})
    assert response.status_code == 400
    assert response.json == {
        "errors": {"test_field": "'' is not one of ['chose1', 'chose2']"},
        "message": "Input payload validation failed",
    }


def test_open_api_definition(client):
    response = client.get("/swagger.json")
    assert response.json == {
        "swagger": "2.0",
        "basePath": "/",
        "paths": {
            "/test": {
                "get": {
                    "responses": {"200": {"description": "Success"}},
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
                            "name": "test_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                            "enum": ["chose1", "chose2"],
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
                            "name": "order_by",
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
                                "$ref": "#/definitions/TestTable_PostRequestModel"
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
                                "$ref": "#/definitions/TestTable_PutRequestModel"
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
                            "name": "test_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                            "enum": ["chose1", "chose2"],
                        },
                    ],
                    "tags": ["Test"],
                },
            },
            "/test_parsers": {
                "delete": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "delete_test_parsers_resource",
                    "tags": ["Test"],
                },
                "get": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "get_test_parsers_resource",
                    "tags": ["Test"],
                },
            },
        },
        "info": {"title": "API", "version": "1.0"},
        "produces": ["application/json"],
        "consumes": ["application/json"],
        "tags": [{"name": "Test"}],
        "definitions": {
            "TestTable_PostRequestModel": {
                "required": ["key"],
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample_value",
                    },
                    "test_field": {
                        "type": "string",
                        "readOnly": False,
                        "example": "chose1",
                        "enum": ["chose1", "chose2"],
                    },
                },
                "type": "object",
            },
            "TestTable_PutRequestModel": {
                "required": ["key"],
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample_value",
                    },
                    "test_field": {
                        "type": "string",
                        "readOnly": False,
                        "example": "chose1",
                        "enum": ["chose1", "chose2"],
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
