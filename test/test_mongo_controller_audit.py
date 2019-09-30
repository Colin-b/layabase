import datetime

import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed

import layabase
import layabase.testing
import layabase.database_mongo
import layabase.audit_mongo
from test import DateTimeModuleMock


@pytest.fixture
def controller():
    class TestModel:
        __tablename__ = "sample_table_name"

        key = layabase.database_mongo.Column(str, is_primary_key=True)
        mandatory = layabase.database_mongo.Column(int, is_nullable=False)
        optional = layabase.database_mongo.Column(str)

    controller = layabase.CRUDController(TestModel, audit=True)
    _db = layabase.load("mongomock?ssl=True", [controller], replicaSet="globaldb")
    yield controller
    layabase.testing.reset(_db)


@pytest.fixture
def app(controller):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    controller.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(controller.query_get_parser)
        @namespace.marshal_with(controller.get_response_model)
        def get(self):
            return []

        @namespace.expect(controller.json_post_model)
        def post(self):
            return []

        @namespace.expect(controller.json_put_model)
        def put(self):
            return []

        @namespace.expect(controller.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test/audit")
    class TestAuditResource(flask_restplus.Resource):
        @namespace.expect(controller.query_get_audit_parser)
        @namespace.marshal_with(controller.get_audit_response_model)
        def get(self):
            return []

    @namespace.route("/test_audit_parser")
    class TestAuditParserResource(flask_restplus.Resource):
        @namespace.expect(controller.query_get_audit_parser)
        def get(self):
            return controller.query_get_audit_parser.parse_args()

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        @namespace.expect(controller.query_get_parser)
        def get(self):
            return controller.query_get_parser.parse_args()

        @namespace.expect(controller.query_delete_parser)
        def delete(self):
            return controller.query_delete_parser.parse_args()

    return application


def test_get_all_without_data_returns_empty_list(controller):
    assert controller.get({}) == []
    assert controller.get_audit({}) == []


def test_audit_table_name_is_forbidden():
    class TestAuditModel:
        __tablename__ = "audit"

        key = layabase.database_mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        layabase.load(
            "mongomock?ssl=True",
            [layabase.CRUDController(TestAuditModel)],
            replicaSet="globaldb",
        )

    assert "audit is a reserved collection name." == str(exception_info.value)


def test_audited_table_name_is_forbidden():
    class TestAuditModel:
        __tablename__ = "audit_int_table_name"

        key = layabase.database_mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        layabase.load(
            "mongomock?ssl=True",
            [layabase.CRUDController(TestAuditModel)],
            replicaSet="globaldb",
        )

    assert (
        str(exception_info.value)
        == "audit_int_table_name is a reserved collection name."
    )


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
                                "$ref": "#/definitions/TestModel_PutRequestModel"
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
                                "$ref": "#/definitions/TestModel_PostRequestModel"
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
                                "$ref": "#/definitions/TestModel_GetResponseModel"
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
            },
            "/test/audit": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestModel_GetAuditResponseModel"
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
            },
        },
        "info": {"title": "API", "version": "1.0"},
        "produces": ["application/json"],
        "consumes": ["application/json"],
        "tags": [{"name": "Test"}],
        "definitions": {
            "TestModel_PutRequestModel": {
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "mandatory": {"type": "integer", "readOnly": False, "example": 1},
                    "optional": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample optional",
                    },
                },
                "type": "object",
            },
            "TestModel_PostRequestModel": {
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "mandatory": {"type": "integer", "readOnly": False, "example": 1},
                    "optional": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample optional",
                    },
                },
                "type": "object",
            },
            "TestModel_GetResponseModel": {
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "mandatory": {"type": "integer", "readOnly": False, "example": 1},
                    "optional": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample optional",
                    },
                },
                "type": "object",
            },
            "TestModel_GetAuditResponseModel": {
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
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                    "mandatory": {"type": "integer", "readOnly": False, "example": 1},
                    "optional": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample optional",
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


def test_post_with_nothing_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert not exception_info.value.received_data
    assert controller.get_audit({}) == []


def test_post_many_with_nothing_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == []
    assert controller.get_audit({}) == []


def test_post_with_empty_dict_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({})
    assert {
        "key": ["Missing data for required field."],
        "mandatory": ["Missing data for required field."],
    } == exception_info.value.errors
    assert exception_info.value.received_data == {}
    assert controller.get_audit({}) == []


def test_post_many_with_empty_list_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many([])
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == []
    assert controller.get_audit({}) == []


def test_put_with_nothing_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert not exception_info.value.received_data
    assert controller.get_audit({}) == []


def test_put_with_empty_dict_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put({})
    assert exception_info.value.errors == {"key": ["Missing data for required field."]}
    assert exception_info.value.received_data == {}
    assert controller.get_audit({}) == []


def test_delete_without_nothing_do_not_fail(controller):
    assert controller.delete({}) == 0
    assert controller.get_audit({}) == []


def test_post_without_mandatory_field_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"key": "my_key"})
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }
    assert exception_info.value.received_data == {"key": "my_key"}
    assert controller.get_audit({}) == []


def test_post_many_without_mandatory_field_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many([{"key": "my_key"}])
    assert exception_info.value.errors == {
        0: {"mandatory": ["Missing data for required field."]}
    }
    assert exception_info.value.received_data == [{"key": "my_key"}]
    assert controller.get_audit({}) == []


def test_post_without_key_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"mandatory": 1})
    assert exception_info.value.errors == {"key": ["Missing data for required field."]}
    assert exception_info.value.received_data == {"mandatory": 1}
    assert controller.get_audit({}) == []


def test_post_many_without_key_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many([{"mandatory": 1}])
    assert exception_info.value.errors == {
        0: {"key": ["Missing data for required field."]}
    }
    assert exception_info.value.received_data == [{"mandatory": 1}]
    assert controller.get_audit({}) == []


def test_post_with_wrong_type_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"key": datetime.date(2007, 12, 5), "mandatory": 1})
    assert exception_info.value.errors == {"key": ["Not a valid str."]}
    assert exception_info.value.received_data == {
        "key": datetime.date(2007, 12, 5),
        "mandatory": 1,
    }
    assert controller.get_audit({}) == []


def test_post_many_with_wrong_type_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many([{"key": datetime.date(2007, 12, 5), "mandatory": 1}])
    assert exception_info.value.errors == {0: {"key": ["Not a valid str."]}}
    assert exception_info.value.received_data == [
        {"key": datetime.date(2007, 12, 5), "mandatory": 1}
    ]
    assert controller.get_audit({}) == []


def test_put_with_wrong_type_is_invalid(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post({"key": "value1", "mandatory": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put({"key": "value1", "mandatory": "invalid_value"})
    assert exception_info.value.errors == {"mandatory": ["Not a valid int."]}
    assert exception_info.value.received_data == {
        "key": "value1",
        "mandatory": "invalid_value",
    }
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "value1",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_post_without_optional_is_valid(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert controller.post({"key": "my_key", "mandatory": 1}) == {
        "optional": None,
        "mandatory": 1,
        "key": "my_key",
    }
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


def test_post_many_without_optional_is_valid(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert controller.post_many([{"key": "my_key", "mandatory": 1}]) == [
        {"optional": None, "mandatory": 1, "key": "my_key"}
    ]
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


def test_put_many_is_valid(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )
    controller.put_many(
        [{"key": "my_key", "optional": "test"}, {"key": "my_key2", "mandatory": 3}]
    )
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": None,
            "revision": 2,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "test",
            "revision": 3,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 3,
            "optional": None,
            "revision": 4,
        },
    ]


def test_post_with_optional_is_valid(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert controller.post(
        {"key": "my_key", "mandatory": 1, "optional": "my_value"}
    ) == {"mandatory": 1, "key": "my_key", "optional": "my_value"}
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_many_with_optional_is_valid(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert controller.post_many(
        [{"key": "my_key", "mandatory": 1, "optional": "my_value"}]
    ) == [{"mandatory": 1, "key": "my_key", "optional": "my_value"}]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_with_unknown_field_is_valid(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert controller.post(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    ) == {"optional": "my_value", "mandatory": 1, "key": "my_key"}
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_many_with_unknown_field_is_valid(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    assert controller.post_many(
        [
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            }
        ]
    ) == [{"optional": "my_value", "mandatory": 1, "key": "my_key"}]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_get_without_filter_is_retrieving_the_only_item(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller.get({}) == [
        {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        }
    ]


def test_get_without_filter_is_retrieving_everything_with_multiple_posts(
    controller, monkeypatch
):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_get_without_filter_is_retrieving_everything(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert controller.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_get_with_filter_is_retrieving_subset(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({"optional": "my_value1"}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_put_is_updating(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller.put({"key": "my_key1", "optional": "my_value"}) == (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    )
    assert controller.get({"mandatory": 1}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 2,
        },
    ]


def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(
    controller, monkeypatch
):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "optional": "my_value"})
    assert controller.get({"optional": "my_value1"}) == []
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 2,
        },
    ]


def test_delete_with_filter_is_removing_the_proper_row(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.delete({"key": "my_key1"}) == 1
    assert controller.get({}) == [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 3,
        },
    ]


def test_audit_filter_on_model_is_returning_only_selected_data(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "mandatory": 2})
    controller.delete({"key": "my_key1"})
    assert controller.get_audit({"key": "my_key1"}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 2,
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 3,
        },
    ]


def test_audit_filter_on_audit_model_is_returning_only_selected_data(
    controller, monkeypatch
):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "mandatory": 2})
    controller.delete({"key": "my_key1"})
    assert controller.get_audit({"audit_action": "Update"}) == [
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 2,
        }
    ]


def test_value_can_be_updated_to_previous_value(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "mandatory": 2})
    controller.put({"key": "my_key1", "mandatory": 1})  # Put back initial value
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 2,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 3,
        },
    ]


def test_delete_without_filter_is_removing_everything(controller, monkeypatch):
    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == controller.delete({})
    assert [] == controller.get({})
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 3,
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 4,
        },
    ]


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
