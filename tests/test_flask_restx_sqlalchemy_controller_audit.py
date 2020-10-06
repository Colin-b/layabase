import pytest
import sqlalchemy
import flask
import flask_restx

import layabase
import layabase._database_sqlalchemy
from layabase.testing import mock_sqlalchemy_audit_datetime


@pytest.fixture
def controller1() -> layabase.CRUDController:
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    return layabase.CRUDController(TestTable, audit=True)


@pytest.fixture
def controller2() -> layabase.CRUDController:
    class TestTable2:
        __tablename__ = "test2"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    return layabase.CRUDController(TestTable2, audit=True)


@pytest.fixture
def controllers(controller1: layabase.CRUDController, controller2: layabase.CRUDController):
    return layabase.load("sqlite:///:memory:", [controller1, controller2])


@pytest.fixture
def app(controllers, controller1: layabase.CRUDController, controller2: layabase.CRUDController):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restx.Api(application)
    namespace = api.namespace("Test", path="/")

    controller1.flask_restx.init_models(namespace)
    controller2.flask_restx.init_models(namespace)

    @namespace.route("/test")
    class TestResource(flask_restx.Resource):
        @namespace.expect(controller1.flask_restx.query_get_parser)
        @namespace.marshal_with(controller1.flask_restx.get_response_model)
        def get(self):
            return []

        @namespace.expect(controller1.flask_restx.json_post_model)
        def post(self):
            return []

        @namespace.expect(controller1.flask_restx.json_put_model)
        def put(self):
            return []

        @namespace.expect(controller1.flask_restx.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test/description")
    class TestDescriptionResource(flask_restx.Resource):
        @namespace.marshal_with(controller1.flask_restx.get_model_description_response_model)
        def get(self):
            return {}

    @namespace.route("/test/audit")
    class TestAuditResource(flask_restx.Resource):
        @namespace.expect(controller1.flask_restx.query_get_audit_parser)
        @namespace.marshal_with(controller1.flask_restx.get_audit_response_model)
        def get(self):
            return []

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restx.Resource):
        def get(self):
            return controller1.flask_restx.query_get_parser.parse_args()

        def delete(self):
            return controller1.flask_restx.query_delete_parser.parse_args()

    @namespace.route("/test_audit_parser")
    class TestAuditParserResource(flask_restx.Resource):
        def get(self):
            return controller1.flask_restx.query_get_audit_parser.parse_args()

    return application


def test_open_api_definition(client, controller1):
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
            },
            "/test/audit": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestTable_GetAuditResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_audit_resource",
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
                            "name": "audit_action",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                            "enum": ["I", "U", "D"],
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
                }
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
            "/test_audit_parser": {
                "get": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "get_test_audit_parser_resource",
                    "tags": ["Test"],
                }
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
            "TestTable_GetAuditResponseModel": {
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
                    "audit_action": {
                        "type": "string",
                        "readOnly": False,
                        "example": "I",
                        "enum": ["I", "U", "D"],
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
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }
    assert controller1.get_audit({}) == []


def test_open_api_definition_without_offset_support(monkeypatch):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restx.Api(application)
    namespace = api.namespace("Test", path="/")

    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    controller = layabase.CRUDController(TestTable, audit=True)
    monkeypatch.setattr(
        layabase._database_sqlalchemy, "_supports_offset", lambda *args: False
    )
    layabase.load("sqlite:///:memory:", [controller])

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

    @namespace.route("/test/audit")
    class TestAuditResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_get_audit_parser)
        @namespace.marshal_with(controller.flask_restx.get_audit_response_model)
        def get(self):
            return []

    with application.test_client() as client:
        response = client.get("/swagger.json")

    assert response.json == {
        "swagger": "2.0",
        "basePath": "/",
        "paths": {
            "/test": {
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
            "/test/audit": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestTable_GetAuditResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_audit_resource",
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
                            "name": "audit_action",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                            "enum": ["I", "U", "D"],
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
                }
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
            "TestTable_GetAuditResponseModel": {
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
                    "audit_action": {
                        "type": "string",
                        "readOnly": False,
                        "example": "I",
                        "enum": ["I", "U", "D"],
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
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }


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


def test_query_get_audit_parser(client):
    response = client.get(
        "/test_audit_parser?key=12&mandatory=123&optional=1234&limit=1&order_by=key&offset=0&audit_action=I&audit_user=test&revision=1"
    )
    assert response.json == {
        "key": ["12"],
        "mandatory": [123],
        "optional": ["1234"],
        "limit": 1,
        "order_by": ["key"],
        "offset": 0,
        "audit_action": ["I"],
        "audit_date_utc": None,
        "audit_user": ["test"],
        "revision": [1],
    }


def test_query_delete_parser(client):
    response = client.delete("/test_parsers?key=12&mandatory=123&optional=1234")
    assert response.json == {"key": ["12"], "mandatory": [123], "optional": ["1234"]}
