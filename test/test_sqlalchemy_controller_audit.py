import pytest
import sqlalchemy
import flask
import flask_restplus
from layaberr import ValidationFailed

import layabase
from layabase.testing import mock_sqlalchemy_audit_datetime


@pytest.fixture
def controller1():
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    return layabase.CRUDController(TestTable, audit=True)


@pytest.fixture
def controller2():
    class TestTable2:
        __tablename__ = "test2"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    return layabase.CRUDController(TestTable2, audit=True)


@pytest.fixture
def controllers(controller1, controller2):
    return layabase.load("sqlite:///:memory:", [controller1, controller2])


@pytest.fixture
def app(controllers, controller1, controller2):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    controller1.namespace(namespace)
    controller2.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(controller1.query_get_parser)
        @namespace.marshal_with(controller1.get_response_model)
        def get(self):
            return []

        @namespace.expect(controller1.json_post_model)
        def post(self):
            return []

        @namespace.expect(controller1.json_put_model)
        def put(self):
            return []

        @namespace.expect(controller1.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test/description")
    class TestDescriptionResource(flask_restplus.Resource):
        @namespace.marshal_with(controller1.get_model_description_response_model)
        def get(self):
            return {}

    @namespace.route("/test/audit")
    class TestAuditResource(flask_restplus.Resource):
        @namespace.expect(controller1.query_get_audit_parser)
        @namespace.marshal_with(controller1.get_audit_response_model)
        def get(self):
            return []

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        def get(self):
            return controller1.query_get_parser.parse_args()

        def delete(self):
            return controller1.query_delete_parser.parse_args()

    @namespace.route("/test_audit_parser")
    class TestAuditParserResource(flask_restplus.Resource):
        def get(self):
            return controller1.query_get_audit_parser.parse_args()

    return application


def test_get_audit_without_providing_a_dictionary(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.get_audit("")
    assert exception_info.value.errors == {"": ["Must be a dictionary."]}
    assert exception_info.value.received_data == ""


def test_get_all_without_data_returns_empty_list(controllers, controller1):
    assert controller1.get({}) == []
    assert controller1.get_audit({}) == []


def test_open_api_definition(client, controller1):
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
                    "key": {"type": "string", "example": "sample_value"},
                    "mandatory": {"type": "integer", "example": 1},
                    "optional": {"type": "string", "example": "sample_value"},
                },
                "type": "object",
            },
            "TestTable_PutRequestModel": {
                "required": ["key", "mandatory"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "mandatory": {"type": "integer", "example": 1},
                    "optional": {"type": "string", "example": "sample_value"},
                },
                "type": "object",
            },
            "TestTable_GetResponseModel": {
                "required": ["key", "mandatory"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "mandatory": {"type": "integer", "example": 1},
                    "optional": {"type": "string", "example": "sample_value"},
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
                    "key": {"type": "string", "example": "sample_value"},
                    "mandatory": {"type": "integer", "example": 1},
                    "optional": {"type": "string", "example": "sample_value"},
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


def test_post_with_nothing_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.post(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_post_many_with_nothing_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.post_many(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_post_with_empty_dict_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.post({})
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_post_many_with_empty_list_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.post_many([])
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_put_with_nothing_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.put(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_put_with_empty_dict_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.put({})
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_delete_without_nothing_do_not_fail(controllers, controller1):
    assert controller1.delete({}) == 0
    assert controller1.get_audit({}) == []


def test_post_without_mandatory_field_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.post({"key": "my_key"})
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }
    assert exception_info.value.received_data == {"key": "my_key"}
    assert controller1.get_audit({}) == []


def test_post_many_without_mandatory_field_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.post_many([{"key": "my_key"}])
    assert exception_info.value.errors == {
        0: {"mandatory": ["Missing data for required field."]}
    }
    assert exception_info.value.received_data == [{"key": "my_key"}]
    assert controller1.get_audit({}) == []


def test_post_without_key_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.post({"mandatory": 1})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {"mandatory": 1} == exception_info.value.received_data
    assert controller1.get_audit({}) == []


def test_post_many_without_key_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.post_many([{"mandatory": 1}])
    assert {
        0: {"key": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [{"mandatory": 1}] == exception_info.value.received_data
    assert controller1.get_audit({}) == []


def test_post_with_wrong_type_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.post({"key": 256, "mandatory": 1})
    assert {"key": ["Not a valid string."]} == exception_info.value.errors
    assert {"key": 256, "mandatory": 1} == exception_info.value.received_data
    assert controller1.get_audit({}) == []


def test_post_many_with_wrong_type_is_invalid(controllers, controller1):
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.post_many([{"key": 256, "mandatory": 1}])
    assert {0: {"key": ["Not a valid string."]}} == exception_info.value.errors
    assert [{"key": 256, "mandatory": 1}] == exception_info.value.received_data
    assert controller1.get_audit({}) == []


def test_put_with_wrong_type_is_invalid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "value1", "mandatory": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        controller1.put({"key": "value1", "mandatory": "invalid_value"})
    assert {"mandatory": ["Not a valid integer."]} == exception_info.value.errors
    assert {
        "key": "value1",
        "mandatory": "invalid_value",
    } == exception_info.value.received_data
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "value1",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_post_without_optional_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert {"optional": None, "mandatory": 1, "key": "my_key"} == controller1.post(
        {"key": "my_key", "mandatory": 1}
    )
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_post_on_a_second_table_without_optional_is_valid(
    controllers, controller1, controller2, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key", "mandatory": 1})
    assert {"optional": None, "mandatory": 1, "key": "my_key"} == controller2.post(
        {"key": "my_key", "mandatory": 1}
    )
    assert controller2.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_post_many_without_optional_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert [
        {"optional": None, "mandatory": 1, "key": "my_key"}
    ] == controller1.post_many([{"key": "my_key", "mandatory": 1}])
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_post_with_optional_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert {
        "mandatory": 1,
        "key": "my_key",
        "optional": "my_value",
    } == controller1.post({"key": "my_key", "mandatory": 1, "optional": "my_value"})
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_many_with_optional_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": "my_value"}
    ] == controller1.post_many(
        [{"key": "my_key", "mandatory": 1, "optional": "my_value"}]
    )
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_with_unknown_field_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert {
        "optional": "my_value",
        "mandatory": 1,
        "key": "my_key",
    } == controller1.post(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    )
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_many_with_unknown_field_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert [
        {"optional": "my_value", "mandatory": 1, "key": "my_key"}
    ] == controller1.post_many(
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
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_get_without_filter_is_retrieving_the_only_item(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert [
        {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
    ] == controller1.get({})
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        }
    ]


def test_get_without_filter_is_retrieving_everything_with_multiple_posts(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == controller1.get({})
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_get_without_filter_is_retrieving_everything(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == controller1.get({})
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_get_with_filter_is_retrieving_subset(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == controller1.get({"optional": "my_value1"})
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_put_is_updating(controllers, controller1, mock_sqlalchemy_audit_datetime):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    ) == controller1.put({"key": "my_key1", "optional": "my_value"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ] == controller1.get({"mandatory": 1})
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "U",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 2,
        },
    ]


def test_put_many_is_updating(controllers, controller1, mock_sqlalchemy_audit_datetime):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert (
        [{"key": "my_key1", "mandatory": 1, "optional": "my_value1"}],
        [{"key": "my_key1", "mandatory": 1, "optional": "my_value"}],
    ) == controller1.put_many([{"key": "my_key1", "optional": "my_value"}])
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ] == controller1.get({"mandatory": 1})
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "U",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 2,
        },
    ]


def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.put({"key": "my_key1", "optional": "my_value"})
    assert [] == controller1.get({"optional": "my_value1"})
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "U",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 2,
        },
    ]


def test_delete_with_filter_is_removing_the_proper_row(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller1.delete({"key": "my_key1"}) == 1
    assert controller1.get({}) == [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
        {
            "audit_action": "D",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 3,
        },
    ]


def test_audit_filter_is_returning_only_selected_data(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.put({"key": "my_key1", "mandatory": 2})
    controller1.delete({"key": "my_key1"})
    assert controller1.get_audit({"key": "my_key1"}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "U",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 2,
        },
        {
            "audit_action": "D",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 3,
        },
    ]


def test_audit_filter_on_audit_table_is_returning_only_selected_data(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.put({"key": "my_key1", "mandatory": 2})
    controller1.delete({"key": "my_key1"})
    assert controller1.get_audit({"audit_action": "U"}) == [
        {
            "audit_action": "U",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 2,
        }
    ]


def test_delete_without_filter_is_removing_everything(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == controller1.delete({})
    assert [] == controller1.get({})
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
        {
            "audit_action": "D",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 3,
        },
        {
            "audit_action": "D",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 4,
        },
    ]


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
