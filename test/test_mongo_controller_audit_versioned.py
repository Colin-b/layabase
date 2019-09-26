import enum
import re

import flask
import flask_restplus
import pytest
from layaberr import ModelCouldNotBeFound

from layabase import database, database_mongo, versioning_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestController(database.CRUDController):
    pass


class TestVersionedController(database.CRUDController):
    pass


def _create_models(base):
    class TestModel(
        database_mongo.CRUDModel, base=base, table_name="sample_table_name", audit=True
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        mandatory = database_mongo.Column(int, is_nullable=False)
        optional = database_mongo.Column(str)

    class TestVersionedModel(
        versioning_mongo.VersionedCRUDModel,
        base=base,
        table_name="versioned_table_name",
        audit=True,
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        enum_fld = database_mongo.Column(EnumTest)

    TestController.model(TestModel)
    TestVersionedController.model(TestVersionedModel)
    return [TestModel, TestVersionedModel]


@pytest.fixture
def db():
    _db = database.load("mongomock?ssl=True", _create_models, replicaSet="globaldb")
    yield _db
    layabase.testing.reset(_db)


@pytest.fixture
def app(db):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    TestVersionedController.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(TestVersionedController.query_get_parser)
        @namespace.marshal_with(TestVersionedController.get_response_model)
        def get(self):
            return []

        @namespace.expect(TestVersionedController.json_post_model)
        def post(self):
            return []

        @namespace.expect(TestVersionedController.json_put_model)
        def put(self):
            return []

        @namespace.expect(TestVersionedController.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test/audit")
    class TestAuditResource(flask_restplus.Resource):
        @namespace.expect(TestVersionedController.query_get_audit_parser)
        @namespace.marshal_with(TestVersionedController.get_audit_response_model)
        def get(self):
            return []

    @namespace.route("/test_audit_parser")
    class TestAuditParserResource(flask_restplus.Resource):
        @namespace.expect(TestVersionedController.query_get_audit_parser)
        def get(self):
            return TestVersionedController.query_get_audit_parser.parse_args()

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
                                "$ref": "#/definitions/TestVersionedModel_Versioned"
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
                "put": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "put_test_resource",
                    "parameters": [
                        {
                            "name": "payload",
                            "required": True,
                            "in": "body",
                            "schema": {
                                "$ref": "#/definitions/TestVersionedModel_Versioned"
                            },
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
                            "schema": {
                                "$ref": "#/definitions/TestVersionedModel_Versioned"
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
                            "schema": {"$ref": "#/definitions/AuditModel"},
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
            "TestVersionedModel_Versioned": {
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
            "AuditModel": {
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


def test_revision_not_shared_if_not_versioned(db):
    assert {"optional": None, "mandatory": 1, "key": "my_key"} == TestController.post(
        {"key": "my_key", "mandatory": 1}
    )
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )
    _check_audit(
        TestVersionedController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 1,
                "table_name": "versioned_table_name",
            }
        ],
    )


def test_revision_on_versionned_audit_after_put_failure(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    with pytest.raises(ModelCouldNotBeFound):
        TestVersionedController.put({"key": "my_key2", "enum_fld": EnumTest.Value2})
    TestVersionedController.delete({"key": "my_key"})
    _check_audit(
        TestVersionedController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 1,
                "table_name": "versioned_table_name",
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 2,
                "table_name": "versioned_table_name",
            },
        ],
    )


def test_versioned_audit_after_post_put_delete_rollback(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    TestVersionedController.delete({"key": "my_key"})
    TestVersionedController.rollback_to({"revision": 1})
    _check_audit(
        TestVersionedController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 1,
                "table_name": "versioned_table_name",
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 2,
                "table_name": "versioned_table_name",
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 3,
                "table_name": "versioned_table_name",
            },
            {
                "audit_action": "Rollback",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 4,
                "table_name": "versioned_table_name",
            },
        ],
    )


def test_get_last_when_empty(db):
    assert TestVersionedController.get_last({}) == {}


def test_get_last_when_single_doc_post(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    assert TestVersionedController.get_last({}) == {
        "enum_fld": "Value1",
        "key": "my_key",
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }


def test_get_last_with_unmatched_filter(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    assert TestVersionedController.get_last({"key": "my_key2"}) == {}


def test_get_last_when_single_update(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    assert TestVersionedController.get_last({}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": -1,
    }


def test_get_last_when_removed(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    TestVersionedController.delete({"key": "my_key"})
    assert TestVersionedController.get_last({}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": 3,
    }


def test_get_last_with_one_removed_and_a_valid(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    TestVersionedController.delete({"key": "my_key"})
    TestVersionedController.post({"key": "my_key2", "enum_fld": EnumTest.Value1})
    assert TestVersionedController.get_last({}) == {
        "enum_fld": "Value1",
        "key": "my_key2",
        "valid_since_revision": 4,
        "valid_until_revision": -1,
    }


def test_get_last_with_one_removed_and_a_valid_and_filter_on_removed(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    TestVersionedController.delete({"key": "my_key"})
    TestVersionedController.post({"key": "my_key2", "enum_fld": EnumTest.Value1})
    assert TestVersionedController.get_last({"key": "my_key"}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": 3,
    }


def _check_audit(controller, expected_audit):
    audit = controller.get_audit({})
    audit = [
        {key: audit_line[key] for key in sorted(audit_line.keys())}
        for audit_line in audit
    ]

    assert re.match(
        f"{expected_audit}".replace("[", "\\[")
        .replace("]", "\\]")
        .replace("\\\\", "\\"),
        f"{audit}",
    )
