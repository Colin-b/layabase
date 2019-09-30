import pytest
import sqlalchemy
import flask
import flask_restplus
from layaberr import ValidationFailed

import layabase
import layabase.testing


@pytest.fixture
def controller():
    class TestRequiredModel:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(
            sqlalchemy.Integer,
            nullable=False,
            info={"marshmallow": {"required_on_query": True}},
        )

    controller = layabase.CRUDController(TestRequiredModel)
    _db = layabase.load("sqlite:///:memory:", [controller])
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

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        def get(self):
            return controller.query_get_parser.parse_args()

        def delete(self):
            return controller.query_delete_parser.parse_args()

    return application


def test_query_get_parser_without_required_field(client):
    response = client.get("/test_parsers")
    assert response.status_code == 400
    assert response.json == {
        "errors": {"mandatory": "Missing required parameter in the query string"},
        "message": "Input payload validation failed",
    }


def test_get_without_required_field(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.get({})
    assert exception_info.value.received_data == {}
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }


def test_get_with_required_field(controller):
    assert controller.get({"mandatory": 1}) == []


def test_get_one_without_required_field(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.get_one({})
    assert exception_info.value.received_data == {}
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }


def test_get_one_with_required_field(controller):
    assert controller.get_one({"mandatory": 1}) == {}


def test_delete_without_required_field(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.delete({})
    assert exception_info.value.received_data == {}
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }


def test_delete_with_required_field(controller):
    assert controller.delete({"mandatory": 1}) == 0


def test_query_get_parser_with_required_field(client):
    response = client.get("/test_parsers?mandatory=1")
    assert response.json == {
        "key": None,
        "limit": None,
        "mandatory": [1],
        "offset": None,
        "order_by": None,
    }


def test_query_delete_parser_without_required_field(client):
    response = client.delete("/test_parsers")
    assert response.status_code == 400
    assert response.json == {
        "errors": {"mandatory": "Missing required parameter in the query string"},
        "message": "Input payload validation failed",
    }


def test_query_delete_parser_with_required_field(client):
    response = client.delete("/test_parsers?mandatory=1")
    assert response.json == {"key": None, "mandatory": [1]}


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
                                "$ref": "#/definitions/TestRequiredModel_GetResponseModel"
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
                            "name": "mandatory",
                            "in": "query",
                            "type": "array",
                            "required": True,
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
                            "name": "order_by",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
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
                                "$ref": "#/definitions/TestRequiredModel_PostRequestModel"
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
                                "$ref": "#/definitions/TestRequiredModel_PutRequestModel"
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
                            "name": "mandatory",
                            "in": "query",
                            "type": "array",
                            "required": True,
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                    ],
                    "tags": ["Test"],
                },
            },
            "/test_parsers": {
                "get": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "get_test_parsers_resource",
                    "tags": ["Test"],
                },
                "delete": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "delete_test_parsers_resource",
                    "tags": ["Test"],
                },
            },
        },
        "info": {"title": "API", "version": "1.0"},
        "produces": ["application/json"],
        "consumes": ["application/json"],
        "tags": [{"name": "Test"}],
        "definitions": {
            "TestRequiredModel_PostRequestModel": {
                "required": ["key", "mandatory"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "mandatory": {"type": "integer", "example": 1},
                },
                "type": "object",
            },
            "TestRequiredModel_PutRequestModel": {
                "required": ["key", "mandatory"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "mandatory": {"type": "integer", "example": 1},
                },
                "type": "object",
            },
            "TestRequiredModel_GetResponseModel": {
                "required": ["key", "mandatory"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "mandatory": {"type": "integer", "example": 1},
                },
                "type": "object",
            },
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }
