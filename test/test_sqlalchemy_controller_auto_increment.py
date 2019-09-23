import pytest
import sqlalchemy
import flask
import flask_restplus

from layabase import database, database_sqlalchemy


class TestAutoIncrementController(database.CRUDController):
    pass


def _create_models(base):
    class TestAutoIncrementModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "auto_increment_table_name"

        key = sqlalchemy.Column(
            sqlalchemy.Integer, primary_key=True, autoincrement=True
        )
        enum_field = sqlalchemy.Column(
            sqlalchemy.Enum("Value1", "Value2"),
            nullable=False,
            doc="Test Documentation",
        )
        optional_with_default = sqlalchemy.Column(
            sqlalchemy.String, default="Test value"
        )

    TestAutoIncrementController.model(TestAutoIncrementModel)
    return [TestAutoIncrementModel]


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

    TestAutoIncrementController.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(TestAutoIncrementController.query_get_parser)
        @namespace.marshal_with(TestAutoIncrementController.get_response_model)
        def get(self):
            return []

        @namespace.expect(TestAutoIncrementController.json_post_model)
        def post(self):
            return []

        @namespace.expect(TestAutoIncrementController.json_put_model)
        def put(self):
            return []

        @namespace.expect(TestAutoIncrementController.query_delete_parser)
        def delete(self):
            return []

    return application


def test_post_with_specified_incremented_field_is_ignored_and_valid(client):
    assert {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    } == TestAutoIncrementController.post({"key": "my_key", "enum_field": "Value1"})


def test_post_many_with_specified_incremented_field_is_ignored_and_valid(client):
    assert [
        {"optional_with_default": "Test value", "enum_field": "Value1", "key": 1},
        {"optional_with_default": "Test value", "enum_field": "Value2", "key": 2},
    ] == TestAutoIncrementController.post_many(
        [
            {"key": "my_key", "enum_field": "Value1"},
            {"key": "my_key", "enum_field": "Value2"},
        ]
    )


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
                            "schema": {"$ref": "#/definitions/TestAutoIncrementModel"},
                        }
                    },
                    "operationId": "get_test_resource",
                    "parameters": [
                        {
                            "name": "key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "enum_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "optional_with_default",
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
                "put": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "put_test_resource",
                    "parameters": [
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": {"$ref": "#/definitions/TestAutoIncrementModel"},
                        }
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
                            "schema": {"$ref": "#/definitions/TestAutoIncrementModel"},
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
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "enum_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "optional_with_default",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
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
            "TestAutoIncrementModel": {
                "required": ["enum_field"],
                "properties": {
                    "key": {"type": "integer", "readOnly": True, "example": "0"},
                    "enum_field": {
                        "type": "string",
                        "description": "Test Documentation",
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
                    },
                    "optional_with_default": {
                        "type": "string",
                        "default": "Test value",
                        "example": "Test value",
                    },
                },
                "type": "object",
            }
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }
