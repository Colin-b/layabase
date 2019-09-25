import re

import pytest
import sqlalchemy
import flask
import flask_restplus
from layaberr import ValidationFailed

from layabase import database, database_sqlalchemy
import layabase.testing


class TestController(database.CRUDController):
    pass


class Test2Controller(database.CRUDController):
    pass


def _create_models(base):
    class TestModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "sample_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    TestModel.audit()

    class Test2Model(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "sample2_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    Test2Model.audit()

    TestController.model(TestModel)
    Test2Controller.model(Test2Model)
    return [TestModel, Test2Model]


@pytest.fixture
def db():
    _db = database.load("sqlite:///:memory:", _create_models)
    yield _db
    layabase.testing.reset(_db)


@pytest.fixture
def app(db):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    TestController.namespace(namespace)
    Test2Controller.namespace(namespace)

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

    @namespace.route("/test/description")
    class TestDescriptionResource(flask_restplus.Resource):
        @namespace.marshal_with(TestController.get_model_description_response_model)
        def get(self):
            return {}

    @namespace.route("/test/audit")
    class TestAuditResource(flask_restplus.Resource):
        @namespace.expect(TestController.query_get_audit_parser)
        @namespace.marshal_with(TestController.get_audit_response_model)
        def get(self):
            return []

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        def get(self):
            return TestController.query_get_parser.parse_args()

        def delete(self):
            return TestController.query_delete_parser.parse_args()

    @namespace.route("/test_audit_parser")
    class TestAuditParserResource(flask_restplus.Resource):
        def get(self):
            return TestController.query_get_audit_parser.parse_args()

    return application


def test_get_audit_without_providing_a_dictionary(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.get_audit("")
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert "" == exception_info.value.received_data


def test_get_all_without_data_returns_empty_list(db):
    assert [] == TestController.get({})
    _check_audit(TestController, [])


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
                            "schema": {"$ref": "#/definitions/TestModel"},
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
                            "schema": {"$ref": "#/definitions/TestModel"},
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
                            "schema": {"$ref": "#/definitions/TestModel"},
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
                            "schema": {"$ref": "#/definitions/AuditTestModel"},
                        }
                    },
                    "operationId": "get_test_audit_resource",
                    "parameters": [
                        {
                            "name": "revision",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "integer"},
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
                            "name": "audit_date_utc",
                            "in": "query",
                            "type": "array",
                            "format": "date-time",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "audit_action",
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
            "/test/description": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {"$ref": "#/definitions/TestModelDescription"},
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
            "TestModel": {
                "required": ["key", "mandatory"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "mandatory": {"type": "integer", "example": 1},
                    "optional": {"type": "string", "example": "sample_value"},
                },
                "type": "object",
            },
            "TestModelDescription": {
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
            "AuditTestModel": {
                "required": ["key", "mandatory"],
                "properties": {
                    "revision": {"type": "integer", "readOnly": True, "example": 1},
                    "audit_user": {"type": "string", "example": "sample_value"},
                    "audit_date_utc": {
                        "type": "string",
                        "format": "date-time",
                        "example": "2017-09-24T15:36:09",
                    },
                    "audit_action": {
                        "type": "string",
                        "example": "I",
                        "enum": ["I", "U", "D"],
                    },
                    "key": {"type": "string", "example": "sample_value"},
                    "mandatory": {"type": "integer", "example": 1},
                    "optional": {"type": "string", "example": "sample_value"},
                },
                "type": "object",
            },
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }
    _check_audit(TestController, [])


def test_post_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_empty_list_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([])
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put({})
    assert {"": ["No data provided."]} == exception_info.value.errors
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
        TestController.post({"key": 256, "mandatory": 1})
    assert {"key": ["Not a valid string."]} == exception_info.value.errors
    assert {"key": 256, "mandatory": 1} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_wrong_type_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([{"key": 256, "mandatory": 1}])
    assert {0: {"key": ["Not a valid string."]}} == exception_info.value.errors
    assert [{"key": 256, "mandatory": 1}] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_wrong_type_is_invalid(db):
    TestController.post({"key": "value1", "mandatory": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put({"key": "value1", "mandatory": "invalid_value"})
    assert {"mandatory": ["Not a valid integer."]} == exception_info.value.errors
    assert {
        "key": "value1",
        "mandatory": "invalid_value",
    } == exception_info.value.received_data
    _check_audit(
        TestController,
        [
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )


def test_post_on_a_second_model_without_optional_is_valid(db):
    TestController.post({"key": "my_key", "mandatory": 1})
    assert {"optional": None, "mandatory": 1, "key": "my_key"} == Test2Controller.post(
        {"key": "my_key", "mandatory": 1}
    )
    _check_audit(
        Test2Controller,
        [
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )
    _check_audit(
        TestController,
        [
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )


def _check_audit(controller, expected_audit, filter_audit=None):
    if not filter_audit:
        filter_audit = {}
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "U",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 2,
            },
        ],
    )


def test_put_many_is_updating(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert (
        [{"key": "my_key1", "mandatory": 1, "optional": "my_value1"}],
        [{"key": "my_key1", "mandatory": 1, "optional": "my_value"}],
    ) == TestController.put_many([{"key": "my_key1", "optional": "my_value"}])
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ] == TestController.get({"mandatory": 1})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "U",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "U",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
            {
                "audit_action": "D",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "U",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 2,
            },
            {
                "audit_action": "D",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
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
                "audit_action": "U",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 2,
            }
        ],
        filter_audit={"audit_action": "U"},
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
            {
                "audit_action": "D",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 3,
            },
            {
                "audit_action": "D",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 4,
            },
        ],
    )


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
