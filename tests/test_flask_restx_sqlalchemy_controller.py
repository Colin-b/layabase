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
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    controller = layabase.CRUDController(TestTable)
    layabase.load("sqlite:///:memory:", [controller])
    return controller


@pytest.fixture
def app(controller: layabase.CRUDController):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restx.Api(application)
    namespace = api.namespace("Test", path="/")

    controller.flask_restx.init_models(namespace)

    @namespace.route("/test")
    class TestResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_get_parser)
        @namespace.marshal_with(controller.flask_restx.get_response_model)
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

    @namespace.route("/test/description")
    class TestDescriptionResource(flask_restx.Resource):
        @namespace.marshal_with(controller.flask_restx.get_model_description_response_model)
        def get(self):
            return {}

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restx.Resource):
        def get(self):
            return controller.flask_restx.query_get_parser.parse_args()

        def delete(self):
            return controller.flask_restx.query_delete_parser.parse_args()

    return application


def test_query_get_parser(client):
    response = client.get(
        "/test_parsers?key=12&mandatory=123&optional=1234&limit=1&order_by=key&offset=0"
    )
    assert response.json == {
        "key": ["12"],
        "mandatory": [123],
        "optional": ["1234"],
        "limit": 1,
        "order_by": ["key"],
        "offset": 0,
    }


def test_query_delete_parser(client):
    response = client.delete("/test_parsers?key=12&mandatory=123&optional=1234")
    assert response.json == {"key": ["12"], "mandatory": [123], "optional": ["1234"]}


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
                            "name": "mandatory",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "optional",
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
                                "$ref": "#/definitions/TestTable_GetResponseModel"
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
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "optional",
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
                            "name": "order_by",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
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
            "/test/description": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestTable_GetDescriptionResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_description_resource",
                    "parameters": [
                        {
                            "name": "X-Fields",
                            "in": "header",
                            "type": "string",
                            "format": "mask",
                            "description": "An optional fields mask",
                        }
                    ],
                    "tags": ["Test"],
                }
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
                "required": ["key", "mandatory"],
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample_value",
                    },
                    "mandatory": {"type": "integer", "readOnly": False, "example": 1},
                    "optional": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample_value",
                    },
                },
                "type": "object",
            },
            "TestTable_PutRequestModel": {
                "required": ["key", "mandatory"],
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample_value",
                    },
                    "mandatory": {"type": "integer", "readOnly": False, "example": 1},
                    "optional": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample_value",
                    },
                },
                "type": "object",
            },
            "TestTable_GetResponseModel": {
                "required": ["key", "mandatory"],
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample_value",
                    },
                    "mandatory": {"type": "integer", "readOnly": False, "example": 1},
                    "optional": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample_value",
                    },
                },
                "type": "object",
            },
            "TestTable_GetDescriptionResponseModel": {
                "required": ["key", "mandatory", "table"],
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "Table name",
                        "example": "table",
                    },
                    "key": {"type": "string", "example": "column"},
                    "mandatory": {"type": "string", "example": "column"},
                    "optional": {"type": "string", "example": "column"},
                },
                "type": "object",
            },
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }
