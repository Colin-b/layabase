import enum
import re

import flask
import flask_restplus
import pytest
from layaberr import ModelCouldNotBeFound

from layabase import database, database_mongo, versioning_mongo


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
    database.reset(_db)


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
        "basePath": "/",
        "consumes": ["application/json"],
        "definitions": {
            "AuditModel": {
                "properties": {
                    "audit_action": {
                        "enum": ["Insert", "Update", "Delete", "Rollback"],
                        "example": "Insert",
                        "readOnly": False,
                        "type": "string",
                    },
                    "audit_date_utc": {
                        "example": "2017-09-24T15:36:09",
                        "format": "date-time",
                        "readOnly": False,
                        "type": "string",
                    },
                    "audit_user": {
                        "example": "sample " "audit_user",
                        "readOnly": False,
                        "type": "string",
                    },
                    "revision": {"example": 1, "readOnly": False, "type": "integer"},
                },
                "type": "object",
            },
            "TestVersionedModel_Versioned": {
                "properties": {
                    "enum_fld": {
                        "enum": ["Value1", "Value2"],
                        "example": "Value1",
                        "readOnly": False,
                        "type": "string",
                    },
                    "key": {
                        "example": "sample " "key",
                        "readOnly": False,
                        "type": "string",
                    },
                },
                "type": "object",
            },
        },
        "info": {"title": "API", "version": "1.0"},
        "paths": {
            "/test": {
                "delete": {
                    "operationId": "delete_test_resource",
                    "parameters": [
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "enum_fld",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        },
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                },
                "get": {
                    "operationId": "get_test_resource",
                    "parameters": [
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "enum_fld",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        },
                        {
                            "exclusiveMinimum": True,
                            "in": "query",
                            "minimum": 0,
                            "name": "limit",
                            "type": "integer",
                        },
                        {
                            "in": "query",
                            "minimum": 0,
                            "name": "offset",
                            "type": "integer",
                        },
                        {
                            "description": "An optional " "fields mask",
                            "format": "mask",
                            "in": "header",
                            "name": "X-Fields",
                            "type": "string",
                        },
                    ],
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestVersionedModel_Versioned"
                            },
                        }
                    },
                    "tags": ["Test"],
                },
                "post": {
                    "operationId": "post_test_resource",
                    "parameters": [
                        {
                            "in": "body",
                            "name": "payload",
                            "required": True,
                            "schema": {
                                "$ref": "#/definitions/TestVersionedModel_Versioned"
                            },
                        }
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                },
                "put": {
                    "operationId": "put_test_resource",
                    "parameters": [
                        {
                            "in": "body",
                            "name": "payload",
                            "required": True,
                            "schema": {
                                "$ref": "#/definitions/TestVersionedModel_Versioned"
                            },
                        }
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                },
            },
            "/test/audit": {
                "get": {
                    "operationId": "get_test_audit_resource",
                    "parameters": [
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "audit_action",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "audit_date_utc",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "audit_user",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "revision",
                            "type": "array",
                        },
                        {
                            "exclusiveMinimum": True,
                            "in": "query",
                            "minimum": 0,
                            "name": "limit",
                            "type": "integer",
                        },
                        {
                            "in": "query",
                            "minimum": 0,
                            "name": "offset",
                            "type": "integer",
                        },
                        {
                            "description": "An optional " "fields mask",
                            "format": "mask",
                            "in": "header",
                            "name": "X-Fields",
                            "type": "string",
                        },
                    ],
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {"$ref": "#/definitions/AuditModel"},
                        }
                    },
                    "tags": ["Test"],
                }
            },
            "/test_audit_parser": {
                "get": {
                    "operationId": "get_test_audit_parser_resource",
                    "parameters": [
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "audit_action",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "audit_date_utc",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "audit_user",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "revision",
                            "type": "array",
                        },
                        {
                            "exclusiveMinimum": True,
                            "in": "query",
                            "minimum": 0,
                            "name": "limit",
                            "type": "integer",
                        },
                        {
                            "in": "query",
                            "minimum": 0,
                            "name": "offset",
                            "type": "integer",
                        },
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                }
            },
        },
        "produces": ["application/json"],
        "responses": {
            "MaskError": {"description": "When any error occurs on mask"},
            "ParseError": {"description": "When a mask can't be parsed"},
        },
        "swagger": "2.0",
        "tags": [{"name": "Test"}],
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
