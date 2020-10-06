import enum

import flask
import flask_restx
import pytest

import layabase
import layabase.mongo
from layabase.testing import mock_mongo_audit_datetime


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        mandatory = layabase.mongo.Column(int, is_nullable=False)
        optional = layabase.mongo.Column(str)

    return layabase.CRUDController(TestCollection, audit=True)


@pytest.fixture
def controller_versioned() -> layabase.CRUDController:
    class TestCollectionVersioned:
        __collection_name__ = "test_versioned"

        key = layabase.mongo.Column(str, is_primary_key=True)
        enum_fld = layabase.mongo.Column(EnumTest)

    return layabase.CRUDController(TestCollectionVersioned, audit=True, history=True)


@pytest.fixture
def controllers(controller: layabase.CRUDController, controller_versioned: layabase.CRUDController):
    return layabase.load("mongomock", [controller, controller_versioned])


@pytest.fixture
def app(controllers, controller_versioned: layabase.CRUDController):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restx.Api(application)
    namespace = api.namespace("Test", path="/")

    controller_versioned.flask_restx.init_models(namespace)

    @namespace.route("/test")
    class TestResource(flask_restx.Resource):
        @namespace.expect(controller_versioned.flask_restx.query_get_parser)
        @namespace.marshal_with(controller_versioned.flask_restx.get_response_model)
        def get(self):
            return []

        @namespace.expect(controller_versioned.flask_restx.json_post_model)
        def post(self):
            return []

        @namespace.expect(controller_versioned.flask_restx.json_put_model)
        def put(self):
            return []

        @namespace.expect(controller_versioned.flask_restx.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test/audit")
    class TestAuditResource(flask_restx.Resource):
        @namespace.expect(controller_versioned.flask_restx.query_get_audit_parser)
        @namespace.marshal_with(controller_versioned.flask_restx.get_audit_response_model)
        def get(self):
            return []

    @namespace.route("/test/history")
    class TestHistoryResource(flask_restx.Resource):
        @namespace.expect(controller_versioned.flask_restx.query_get_history_parser)
        @namespace.marshal_with(controller_versioned.flask_restx.get_history_response_model)
        def get(self):
            return []

    @namespace.route("/test/rollback")
    class TestRollbackResource(flask_restx.Resource):
        @namespace.expect(controller_versioned.flask_restx.query_rollback_parser)
        def get(self):
            return []

    @namespace.route("/test_audit_parser")
    class TestAuditParserResource(flask_restx.Resource):
        @namespace.expect(controller_versioned.flask_restx.query_get_audit_parser)
        def get(self):
            return controller_versioned.flask_restx.query_get_audit_parser.parse_args()

    return application


def test_get_versioned_audit_parser_fields(client):
    response = client.get(
        "/test_audit_parser?audit_action=Insert&audit_user=test&limit=1&offset=0&revision=1"
    )
    assert response.json == {
        "audit_action": ["Insert"],
        "audit_date_utc": None,
        "audit_user": ["test"],
        "limit": 1,
        "offset": 0,
        "revision": [1],
    }


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
                                "$ref": "#/definitions/TestCollectionVersioned_GetResponseModel"
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
                            "name": "enum_fld",
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
                "post": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "post_test_resource",
                    "parameters": [
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": {
                                "$ref": "#/definitions/TestCollectionVersioned_PostRequestModel"
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
                            "name": "enum_fld",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
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
                            "schema": {
                                "$ref": "#/definitions/TestCollectionVersioned_PutRequestModel"
                            },
                        }
                    ],
                    "tags": ["Test"],
                },
            },
            "/test/audit": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestCollectionVersioned_GetAuditResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_audit_resource",
                    "parameters": [
                        {
                            "name": "audit_action",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                            "enum": ["Insert", "Update", "Delete", "Rollback"],
                        },
                        {
                            "name": "audit_date_utc",
                            "in": "query",
                            "type": "array",
                            "format": "date-time",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "audit_user",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "revision",
                            "in": "query",
                            "type": "array",
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
                }
            },
            "/test/history": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestCollectionVersioned_GetHistoryResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_history_resource",
                    "parameters": [
                        {
                            "name": "valid_since_revision",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "valid_until_revision",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
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
                            "name": "enum_fld",
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
                }
            },
            "/test/rollback": {
                "get": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "get_test_rollback_resource",
                    "parameters": [
                        {
                            "name": "key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "enum_fld",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "revision",
                            "in": "query",
                            "type": "integer",
                            "minimum": 0,
                            "exclusiveMinimum": True,
                            "required": True,
                        },
                    ],
                    "tags": ["Test"],
                }
            },
            "/test_audit_parser": {
                "get": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "get_test_audit_parser_resource",
                    "parameters": [
                        {
                            "name": "audit_action",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                            "enum": ["Insert", "Update", "Delete", "Rollback"],
                        },
                        {
                            "name": "audit_date_utc",
                            "in": "query",
                            "type": "array",
                            "format": "date-time",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "audit_user",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "revision",
                            "in": "query",
                            "type": "array",
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
                            "name": "offset",
                            "in": "query",
                            "type": "integer",
                            "minimum": 0,
                        },
                    ],
                    "tags": ["Test"],
                }
            },
        },
        "info": {"title": "API", "version": "1.0"},
        "produces": ["application/json"],
        "consumes": ["application/json"],
        "tags": [{"name": "Test"}],
        "definitions": {
            "TestCollectionVersioned_PostRequestModel": {
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "enum_fld": {
                        "type": "string",
                        "readOnly": False,
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
                    },
                },
                "type": "object",
            },
            "TestCollectionVersioned_PutRequestModel": {
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "enum_fld": {
                        "type": "string",
                        "readOnly": False,
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
                    },
                },
                "type": "object",
            },
            "TestCollectionVersioned_GetResponseModel": {
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "enum_fld": {
                        "type": "string",
                        "readOnly": False,
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
                    },
                },
                "type": "object",
            },
            "TestCollectionVersioned_GetAuditResponseModel": {
                "properties": {
                    "audit_action": {
                        "type": "string",
                        "readOnly": False,
                        "example": "Insert",
                        "enum": ["Insert", "Update", "Delete", "Rollback"],
                    },
                    "audit_date_utc": {
                        "type": "string",
                        "format": "date-time",
                        "readOnly": False,
                        "example": "2017-09-24T15:36:09",
                    },
                    "audit_user": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample audit_user",
                    },
                    "revision": {"type": "integer", "readOnly": True, "example": 1},
                },
                "type": "object",
            },
            "TestCollectionVersioned_GetHistoryResponseModel": {
                "properties": {
                    "valid_since_revision": {
                        "type": "integer",
                        "description": "Record is valid since this revision (included).",
                        "readOnly": False,
                        "example": 1,
                    },
                    "valid_until_revision": {
                        "type": "integer",
                        "description": "Record is valid until this revision (excluded).",
                        "readOnly": False,
                        "example": 1,
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "enum_fld": {
                        "type": "string",
                        "readOnly": False,
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
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
