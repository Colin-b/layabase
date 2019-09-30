import enum

import flask
import flask_restplus
import pytest
from layaberr import ModelCouldNotBeFound

import layabase
import layabase.database_mongo
import layabase.testing
import layabase.audit_mongo
from test import DateTimeModuleMock


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller():
    class TestController(layabase.CRUDController):
        class TestModel:
            __tablename__ = "sample_table_name"

            key = layabase.database_mongo.Column(str, is_primary_key=True)
            mandatory = layabase.database_mongo.Column(int, is_nullable=False)
            optional = layabase.database_mongo.Column(str)

        model = TestModel
        audit = True

    return TestController


@pytest.fixture
def controller_versioned():
    class TestVersionedController(layabase.CRUDController):
        class TestVersionedModel:
            __tablename__ = "versioned_table_name"

            key = layabase.database_mongo.Column(str, is_primary_key=True)
            enum_fld = layabase.database_mongo.Column(EnumTest)

        model = TestVersionedModel
        history = True
        audit = True

    return TestVersionedController


@pytest.fixture
def controllers(controller, controller_versioned):
    _db = layabase.load(
        "mongomock?ssl=True", [controller, controller_versioned], replicaSet="globaldb"
    )
    yield _db
    layabase.testing.reset(_db)


@pytest.fixture
def app(controllers, controller_versioned):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    controller_versioned.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(controller_versioned.query_get_parser)
        @namespace.marshal_with(controller_versioned.get_response_model)
        def get(self):
            return []

        @namespace.expect(controller_versioned.json_post_model)
        def post(self):
            return []

        @namespace.expect(controller_versioned.json_put_model)
        def put(self):
            return []

        @namespace.expect(controller_versioned.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test/audit")
    class TestAuditResource(flask_restplus.Resource):
        @namespace.expect(controller_versioned.query_get_audit_parser)
        @namespace.marshal_with(controller_versioned.get_audit_response_model)
        def get(self):
            return []

    @namespace.route("/test_audit_parser")
    class TestAuditParserResource(flask_restplus.Resource):
        @namespace.expect(controller_versioned.query_get_audit_parser)
        def get(self):
            return controller_versioned.query_get_audit_parser.parse_args()

    return application


def test_get_versioned_audit_parser_fields(client):
    response = client.get(
        "/test_audit_parser?audit_action=I&audit_user=test&limit=1&offset=0&revision=1"
    )
    assert response.json == {
        "audit_action": ["I"],
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
                "put": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "put_test_resource",
                    "parameters": [
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": {
                                "$ref": "#/definitions/TestVersionedModel_PutRequestModel"
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
                            "name": "enum_fld",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "key",
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
                                "$ref": "#/definitions/TestVersionedModel_GetResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_resource",
                    "parameters": [
                        {
                            "name": "enum_fld",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
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
                                "$ref": "#/definitions/TestVersionedModel_PostRequestModel"
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
                                "$ref": "#/definitions/TestVersionedModel_GetAuditResponseModel"
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
            "TestVersionedModel_PutRequestModel": {
                "properties": {
                    "enum_fld": {
                        "type": "string",
                        "readOnly": False,
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                },
                "type": "object",
            },
            "TestVersionedModel_PostRequestModel": {
                "properties": {
                    "enum_fld": {
                        "type": "string",
                        "readOnly": False,
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                },
                "type": "object",
            },
            "TestVersionedModel_GetResponseModel": {
                "properties": {
                    "enum_fld": {
                        "type": "string",
                        "readOnly": False,
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                },
                "type": "object",
            },
            "TestVersionedModel_GetAuditResponseModel": {
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
                    "revision": {"type": "integer", "readOnly": False, "example": 1},
                },
                "type": "object",
            },
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }


def test_revision_not_shared_if_not_versioned(
    controllers, controller, controller_versioned, monkeypatch
):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert {"optional": None, "mandatory": 1, "key": "my_key"} == controller.post(
        {"key": "my_key", "mandatory": 1}
    )
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]
    assert controller_versioned.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 1,
            "table_name": "versioned_table_name",
        }
    ]


def test_revision_on_versionned_audit_after_put_failure(
    controllers, controller_versioned, monkeypatch
):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    with pytest.raises(ModelCouldNotBeFound):
        controller_versioned.put({"key": "my_key2", "enum_fld": EnumTest.Value2})
    controller_versioned.delete({"key": "my_key"})
    assert controller_versioned.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 1,
            "table_name": "versioned_table_name",
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 2,
            "table_name": "versioned_table_name",
        },
    ]


def test_versioned_audit_after_post_put_delete_rollback(
    controllers, controller_versioned, monkeypatch
):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller_versioned.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    controller_versioned.delete({"key": "my_key"})
    controller_versioned.rollback_to({"revision": 1})
    assert controller_versioned.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 1,
            "table_name": "versioned_table_name",
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 2,
            "table_name": "versioned_table_name",
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 3,
            "table_name": "versioned_table_name",
        },
        {
            "audit_action": "Rollback",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "revision": 4,
            "table_name": "versioned_table_name",
        },
    ]


def test_get_last_when_empty(controllers, controller_versioned):
    assert controller_versioned.get_last({}) == {}


def test_get_last_when_single_doc_post(controllers, controller_versioned):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    assert controller_versioned.get_last({}) == {
        "enum_fld": "Value1",
        "key": "my_key",
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }


def test_get_last_with_unmatched_filter(controllers, controller_versioned):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    assert controller_versioned.get_last({"key": "my_key2"}) == {}


def test_get_last_when_single_update(controllers, controller_versioned):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller_versioned.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    assert controller_versioned.get_last({}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": -1,
    }


def test_get_last_when_removed(controllers, controller_versioned):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller_versioned.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    controller_versioned.delete({"key": "my_key"})
    assert controller_versioned.get_last({}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": 3,
    }


def test_get_last_with_one_removed_and_a_valid(controllers, controller_versioned):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller_versioned.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    controller_versioned.delete({"key": "my_key"})
    controller_versioned.post({"key": "my_key2", "enum_fld": EnumTest.Value1})
    assert controller_versioned.get_last({}) == {
        "enum_fld": "Value1",
        "key": "my_key2",
        "valid_since_revision": 4,
        "valid_until_revision": -1,
    }


def test_get_last_with_one_removed_and_a_valid_and_filter_on_removed(
    controllers, controller_versioned
):
    controller_versioned.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller_versioned.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    controller_versioned.delete({"key": "my_key"})
    controller_versioned.post({"key": "my_key2", "enum_fld": EnumTest.Value1})
    assert controller_versioned.get_last({"key": "my_key"}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": 3,
    }
