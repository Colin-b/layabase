import flask
import flask_restplus
import pytest
import sqlalchemy

import layabase
import layabase.testing


class TestFloatController(layabase.CRUDController):
    class TestFloatModel:
        __tablename__ = "float_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        float_field = sqlalchemy.Column(sqlalchemy.Float)

    model = TestFloatModel


@pytest.fixture
def db():
    _db = layabase.load("sqlite:///:memory:", [TestFloatController])
    yield _db
    layabase.testing.reset(_db)


@pytest.fixture
def app(db):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    TestFloatController.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(TestFloatController.query_get_parser)
        @namespace.marshal_with(TestFloatController.get_response_model)
        def get(self):
            return []

        @namespace.expect(TestFloatController.json_post_model)
        def post(self):
            return []

        @namespace.expect(TestFloatController.json_put_model)
        def put(self):
            return []

        @namespace.expect(TestFloatController.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        @namespace.expect(TestFloatController.query_get_parser)
        def get(self):
            return TestFloatController.query_get_parser.parse_args()

        @namespace.expect(TestFloatController.query_delete_parser)
        def delete(self):
            return TestFloatController.query_delete_parser.parse_args()

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
                                "$ref": "#/definitions/TestFloatModel_GetResponseModel"
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
                            "name": "float_field",
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
                                "$ref": "#/definitions/TestFloatModel_PostRequestModel"
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
                                "$ref": "#/definitions/TestFloatModel_PutRequestModel"
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
                            "name": "float_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "number"},
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
                    "parameters": [
                        {
                            "name": "key",
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
                            "name": "float_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "number"},
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
            "TestFloatModel_PostRequestModel": {
                "required": ["key"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "float_field": {"type": "number", "example": 1.4},
                },
                "type": "object",
            },
            "TestFloatModel_PutRequestModel": {
                "required": ["key"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "float_field": {"type": "number", "example": 1.4},
                },
                "type": "object",
            },
            "TestFloatModel_GetResponseModel": {
                "required": ["key"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "float_field": {"type": "number", "example": 1.4},
                },
                "type": "object",
            },
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }


def test_query_get_parser(client):
    response = client.get(
        "/test_parsers?key=12&float_field=123.4&limit=1&order_by=key&offset=0"
    )
    assert response.json == {
        "float_field": [123.4],
        "key": ["12"],
        "limit": 1,
        "offset": 0,
        "order_by": ["key"],
    }


def test_query_delete_parser(client):
    response = client.delete("/test_parsers?key=12&float_field=123.4")
    assert response.json == {"float_field": [123.4], "key": ["12"]}
