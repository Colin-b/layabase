import flask
import flask_restplus
import pytest
import sqlalchemy

from layabase import database, database_sqlalchemy


class TestBooleanController(database.CRUDController):
    pass


def _create_models(base):
    class TestBooleanModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "bool_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        bool_field = sqlalchemy.Column(sqlalchemy.Boolean)

    TestBooleanController.model(TestBooleanModel)
    return [TestBooleanModel]


@pytest.fixture
def db():
    _db = database.load("sqlite:///:memory:", _create_models)
    yield _db
    database.reset(_db)


@pytest.fixture
def app(db):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    TestBooleanController.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(TestBooleanController.query_get_parser)
        @namespace.marshal_with(TestBooleanController.get_response_model)
        def get(self):
            return []

        @namespace.expect(TestBooleanController.json_post_model)
        def post(self):
            return []

        @namespace.expect(TestBooleanController.json_put_model)
        def put(self):
            return []

        @namespace.expect(TestBooleanController.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        @namespace.expect(TestBooleanController.query_get_parser)
        def get(self):
            return TestBooleanController.query_get_parser.parse_args()

        @namespace.expect(TestBooleanController.query_delete_parser)
        def delete(self):
            return TestBooleanController.query_delete_parser.parse_args()

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
                            "schema": {"$ref": "#/definitions/TestBooleanModel"},
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
                            "schema": {"$ref": "#/definitions/TestBooleanModel"},
                        }
                    ],
                    "tags": ["Test"],
                },
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {"$ref": "#/definitions/TestBooleanModel"},
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
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
            "TestBooleanModel": {
                "required": ["key"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "bool_field": {"type": "boolean", "example": "true"},
                },
                "type": "object",
            }
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }


def test_query_get_parser(client):
    response = client.get(
        "/test_parsers?key=12&bool_field=true&limit=1&order_by=key&offset=0"
    )
    assert response.json == {
        "bool_field": [True],
        "key": ["12"],
        "limit": 1,
        "offset": 0,
        "order_by": ["key"],
    }


def test_query_delete_parser(client):
    response = client.delete("/test_parsers?key=12&bool_field=true")
    assert response.json == {"bool_field": [True], "key": ["12"]}
