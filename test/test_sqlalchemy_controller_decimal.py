import flask
import flask_restplus
import pytest
import sqlalchemy

import layabase
import layabase.testing


@pytest.fixture
def controller():
    class TestController(layabase.CRUDController):
        class TestDecimalModel:
            __tablename__ = "decimal_table_name"

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            decimal_field = sqlalchemy.Column(sqlalchemy.DECIMAL)

        model = TestDecimalModel

    _db = layabase.load("sqlite:///:memory:", [TestController])
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
                                "$ref": "#/definitions/TestDecimalModel_GetResponseModel"
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
                            "name": "decimal_field",
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
                                "$ref": "#/definitions/TestDecimalModel_PostRequestModel"
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
                                "$ref": "#/definitions/TestDecimalModel_PutRequestModel"
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
                            "name": "decimal_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "number"},
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
            "TestDecimalModel_PostRequestModel": {
                "required": ["key"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "decimal_field": {"type": "number", "example": 1.4},
                },
                "type": "object",
            },
            "TestDecimalModel_PutRequestModel": {
                "required": ["key"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "decimal_field": {"type": "number", "example": 1.4},
                },
                "type": "object",
            },
            "TestDecimalModel_GetResponseModel": {
                "required": ["key"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "decimal_field": {"type": "number", "example": 1.4},
                },
                "type": "object",
            },
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }
