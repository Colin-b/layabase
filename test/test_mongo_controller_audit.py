import datetime
import re

import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo


class TestController(database.CRUDController):
    pass


def _create_models(base):
    class TestModel(
        database_mongo.CRUDModel, base=base, table_name="sample_table_name", audit=True
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        mandatory = database_mongo.Column(int, is_nullable=False)
        optional = database_mongo.Column(str)

    TestController.model(TestModel)
    return [TestModel]


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

    TestController.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(TestController.query_get_parser)
        @namespace.marshal_with(TestController.get_response_model)
        def get(self):
            return []

        @namespace.expect(TestController.json_post_model)
        def post(self):
            return []

        @namespace.expect(TestController.json_put_model)
        def put(self):
            return []

        @namespace.expect(TestController.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test/audit")
    class TestAuditResource(flask_restplus.Resource):
        @namespace.expect(TestController.query_get_audit_parser)
        @namespace.marshal_with(TestController.get_audit_response_model)
        def get(self):
            return []

    @namespace.route("/test_audit_parser")
    class TestAuditParserResource(flask_restplus.Resource):
        @namespace.expect(TestController.query_get_audit_parser)
        def get(self):
            return TestController.query_get_audit_parser.parse_args()

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        @namespace.expect(TestController.query_get_parser)
        def get(self):
            return TestController.query_get_parser.parse_args()

        @namespace.expect(TestController.query_delete_parser)
        def delete(self):
            return TestController.query_delete_parser.parse_args()

    return application


def test_get_all_without_data_returns_empty_list(db):
    assert [] == TestController.get({})
    _check_audit(TestController, [])


def test_audit_table_name_is_forbidden(db):
    with pytest.raises(Exception) as exception_info:

        class TestAuditModel(database_mongo.CRUDModel, base=db, table_name="audit"):
            key = database_mongo.Column(str)

    assert "audit is a reserved collection name." == str(exception_info.value)


def test_audited_table_name_is_forbidden(db):
    with pytest.raises(Exception) as exception_info:

        class TestAuditModel(
            database_mongo.CRUDModel, base=db, table_name="audit_int_table_name"
        ):
            key = database_mongo.Column(str)

    assert "audit_int_table_name is a reserved collection name." == str(
        exception_info.value
    )


def test_open_api_definition(client):
    response = client.get("/swagger.json")
    assert response.json == {
        "basePath": "/",
        "consumes": ["application/json"],
        "definitions": {
            "AuditTestModel": {
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
                    "key": {
                        "example": "sample " "key",
                        "readOnly": False,
                        "type": "string",
                    },
                    "mandatory": {"example": 1, "readOnly": False, "type": "integer"},
                    "optional": {
                        "example": "sample " "optional",
                        "readOnly": False,
                        "type": "string",
                    },
                    "revision": {"example": 1, "readOnly": False, "type": "integer"},
                },
                "type": "object",
            },
            "TestModel": {
                "properties": {
                    "key": {
                        "example": "sample key",
                        "readOnly": False,
                        "type": "string",
                    },
                    "mandatory": {"example": 1, "readOnly": False, "type": "integer"},
                    "optional": {
                        "example": "sample " "optional",
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
                            "name": "key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "mandatory",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "optional",
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
                            "name": "key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "mandatory",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "optional",
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
                            "schema": {"$ref": "#/definitions/TestModel"},
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
                            "schema": {"$ref": "#/definitions/TestModel"},
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
                            "schema": {"$ref": "#/definitions/TestModel"},
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
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "mandatory",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "optional",
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
                            "schema": {"$ref": "#/definitions/AuditTestModel"},
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
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "mandatory",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "optional",
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
            "/test_parsers": {
                "delete": {
                    "operationId": "delete_test_parsers_resource",
                    "parameters": [
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "mandatory",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "optional",
                            "type": "array",
                        },
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                },
                "get": {
                    "operationId": "get_test_parsers_resource",
                    "parameters": [
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "key",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "mandatory",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "optional",
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
                },
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


def test_post_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert not exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert [] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({})
    assert {
        "key": ["Missing data for required field."],
        "mandatory": ["Missing data for required field."],
    } == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_empty_list_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([])
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert [] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert not exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put({})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_delete_without_nothing_do_not_fail(db):
    assert 0 == TestController.delete({})
    _check_audit(TestController, [])


def test_post_without_mandatory_field_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({"key": "my_key"})
    assert {
        "mandatory": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "my_key"} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_without_mandatory_field_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([{"key": "my_key"}])
    assert {
        0: {"mandatory": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [{"key": "my_key"}] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_without_key_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({"mandatory": 1})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {"mandatory": 1} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_without_key_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([{"mandatory": 1}])
    assert {
        0: {"key": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [{"mandatory": 1}] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_with_wrong_type_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({"key": datetime.date(2007, 12, 5), "mandatory": 1})
    assert {"key": ["Not a valid str."]} == exception_info.value.errors
    assert {
        "key": datetime.date(2007, 12, 5),
        "mandatory": 1,
    } == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_wrong_type_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([{"key": datetime.date(2007, 12, 5), "mandatory": 1}])
    assert {0: {"key": ["Not a valid str."]}} == exception_info.value.errors
    assert [
        {"key": datetime.date(2007, 12, 5), "mandatory": 1}
    ] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_wrong_type_is_invalid(db):
    TestController.post({"key": "value1", "mandatory": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put({"key": "value1", "mandatory": "invalid_value"})
    assert {"mandatory": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "key": "value1",
        "mandatory": "invalid_value",
    } == exception_info.value.received_data
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "value1",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )


def test_post_without_optional_is_valid(db):
    assert {"optional": None, "mandatory": 1, "key": "my_key"} == TestController.post(
        {"key": "my_key", "mandatory": 1}
    )
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


def test_post_many_without_optional_is_valid(db):
    assert [
        {"optional": None, "mandatory": 1, "key": "my_key"}
    ] == TestController.post_many([{"key": "my_key", "mandatory": 1}])
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


def test_put_many_is_valid(db):
    TestController.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )
    TestController.put_many(
        [{"key": "my_key", "optional": "test"}, {"key": "my_key2", "mandatory": 3}]
    )
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
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": None,
                "revision": 2,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": "test",
                "revision": 3,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 3,
                "optional": None,
                "revision": 4,
            },
        ],
    )


def _check_audit(controller, expected_audit, filter_audit={}):
    audit = controller.get_audit(filter_audit)
    audit = [
        {key: audit_line[key] for key in sorted(audit_line.keys())}
        for audit_line in audit
    ]

    if not expected_audit:
        assert audit == expected_audit
    else:
        assert re.match(
            f"{expected_audit}".replace("[", "\\[")
            .replace("]", "\\]")
            .replace("\\\\", "\\"),
            f"{audit}",
        )


def test_post_with_optional_is_valid(db):
    assert {
        "mandatory": 1,
        "key": "my_key",
        "optional": "my_value",
    } == TestController.post({"key": "my_key", "mandatory": 1, "optional": "my_value"})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 1,
            }
        ],
    )


def test_post_many_with_optional_is_valid(db):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": "my_value"}
    ] == TestController.post_many(
        [{"key": "my_key", "mandatory": 1, "optional": "my_value"}]
    )
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 1,
            }
        ],
    )


def test_post_with_unknown_field_is_valid(db):
    assert {
        "optional": "my_value",
        "mandatory": 1,
        "key": "my_key",
    } == TestController.post(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    )
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 1,
            }
        ],
    )


def test_post_many_with_unknown_field_is_valid(db):
    assert [
        {"optional": "my_value", "mandatory": 1, "key": "my_key"}
    ] == TestController.post_many(
        [
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            }
        ]
    )
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 1,
            }
        ],
    )


def test_get_without_filter_is_retrieving_the_only_item(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert [
        {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
    ] == TestController.get({})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            }
        ],
    )


def test_get_without_filter_is_retrieving_everything_with_multiple_posts(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
        ],
    )


def test_get_without_filter_is_retrieving_everything(db):
    TestController.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
        ],
    )


def test_get_with_filter_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == TestController.get({"optional": "my_value1"})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
        ],
    )


def test_put_is_updating(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    ) == TestController.put({"key": "my_key1", "optional": "my_value"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ] == TestController.get({"mandatory": 1})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 2,
            },
        ],
    )


def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.put({"key": "my_key1", "optional": "my_value"})
    assert [] == TestController.get({"optional": "my_value1"})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 2,
            },
        ],
    )


def test_delete_with_filter_is_removing_the_proper_row(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 1 == TestController.delete({"key": "my_key1"})
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ] == TestController.get({})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 3,
            },
        ],
    )


def test_audit_filter_on_model_is_returning_only_selected_data(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.put({"key": "my_key1", "mandatory": 2})
    TestController.delete({"key": "my_key1"})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 2,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 3,
            },
        ],
        filter_audit={"key": "my_key1"},
    )


def test_audit_filter_on_audit_model_is_returning_only_selected_data(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.put({"key": "my_key1", "mandatory": 2})
    TestController.delete({"key": "my_key1"})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 2,
            }
        ],
        filter_audit={"audit_action": "Update"},
    )


def test_value_can_be_updated_to_previous_value(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.put({"key": "my_key1", "mandatory": 2})
    TestController.put({"key": "my_key1", "mandatory": 1})  # Put back initial value
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 2,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 3,
            },
        ],
    )


def test_delete_without_filter_is_removing_everything(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == TestController.delete({})
    assert [] == TestController.get({})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 3,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 4,
            },
        ],
    )


def test_query_get_parser(client):
    response = client.get("/test_parsers?key=1&mandatory=2&optional=3&limit=1&offset=0")
    assert response.json == {
        "key": ["1"],
        "limit": 1,
        "mandatory": [2],
        "offset": 0,
        "optional": ["3"],
    }


def test_query_get_audit_parser(client):
    response = client.get(
        "/test_audit_parser?key=1&mandatory=2&optional=3&limit=1&offset=0&revision=1&audit_action=U&audit_user=test"
    )
    assert response.json == {
        "audit_action": ["U"],
        "audit_date_utc": None,
        "audit_user": ["test"],
        "key": ["1"],
        "limit": 1,
        "mandatory": [2],
        "offset": 0,
        "optional": ["3"],
        "revision": [1],
    }


def test_query_delete_parser(client):
    response = client.delete("/test_parsers?key=1&mandatory=2&optional=3")
    assert response.json == {"key": ["1"], "mandatory": [2], "optional": ["3"]}
