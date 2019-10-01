import enum

import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed

import layabase
import layabase.database_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller():
    class TestModel:
        __tablename__ = "test"

        key = layabase.database_mongo.Column(is_primary_key=True)
        dict_field = layabase.database_mongo.DictColumn(
            fields={
                "first_key": layabase.database_mongo.Column(
                    EnumTest, is_nullable=False
                ),
                "second_key": layabase.database_mongo.Column(int, is_nullable=False),
            },
            is_required=True,
        )

    controller = layabase.CRUDController(TestModel, history=True)
    _db = layabase.load("mongomock", [controller])
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

    @namespace.route("/test_rollback_parser")
    class TestRollbackParserResource(flask_restplus.Resource):
        @namespace.expect(controller.query_rollback_parser)
        def get(self):
            return controller.query_rollback_parser.parse_args()

    return application


def test_get_url_with_primary_key_in_model_and_many_models(controller):
    models = [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        },
        {
            "key": "second",
            "dict_field": {"first_key": "Value2", "second_key": 2},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        },
    ]
    assert controller.get_url("/test", *models) == "/test?key=first&key=second"


def test_get_url_with_primary_key_in_model_and_a_single_model(controller):
    model = {
        "key": "first",
        "dict_field": {"first_key": "Value1", "second_key": 1},
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }
    assert controller.get_url("/test", model) == "/test?key=first"


def test_get_url_with_primary_key_in_model_and_no_model(controller):
    assert controller.get_url("/test") == "/test"


def test_post_versioning_is_valid(controller):
    assert {
        "key": "first",
        "dict_field": {"first_key": "Value1", "second_key": 1},
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    } == controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert controller.get_history({}) == [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        }
    ]
    assert controller.get({}) == [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        }
    ]


def test_post_without_providing_required_nullable_dict_column_is_valid(controller):
    assert controller.post({"key": "first"}) == {
        "dict_field": {"first_key": None, "second_key": None},
        "key": "first",
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }


def test_put_without_providing_required_nullable_dict_column_is_valid(controller):
    controller.post(
        {"key": "first", "dict_field": {"first_key": "Value1", "second_key": 0}}
    )
    assert (
        {
            "dict_field": {"first_key": "Value1", "second_key": 0},
            "key": "first",
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 0},
            "key": "first",
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
    ) == controller.put({"key": "first"})


def test_put_versioning_is_valid(controller):
    controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert (
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
    ) == controller.put({"key": "first", "dict_field.first_key": EnumTest.Value2})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
    ] == controller.get_history({})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        }
    ] == controller.get({})


def test_delete_versioning_is_valid(controller):
    controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    controller.put({"key": "first", "dict_field.first_key": EnumTest.Value2})
    assert 1 == controller.delete({"key": "first"})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": 3,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
    ] == controller.get_history({})
    assert [] == controller.get({})


def test_rollback_deleted_versioning_is_valid(controller):
    controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    controller.put({"key": "first", "dict_field.first_key": EnumTest.Value2})
    before_delete = 2
    controller.delete({"key": "first"})
    assert 1 == controller.rollback_to({"revision": before_delete})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": 3,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 4,
            "valid_until_revision": -1,
        },
    ] == controller.get_history({})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 4,
            "valid_until_revision": -1,
        }
    ] == controller.get({})


def test_rollback_before_update_deleted_versioning_is_valid(controller):
    controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    before_update = 1
    controller.put({"key": "first", "dict_field.first_key": EnumTest.Value2})
    controller.delete({"key": "first"})
    assert 1 == controller.rollback_to({"revision": before_update})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": 3,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 4,
            "valid_until_revision": -1,
        },
    ] == controller.get_history({})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 4,
            "valid_until_revision": -1,
        }
    ] == controller.get({})


def test_rollback_already_valid_versioning_is_valid(controller):
    controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    controller.put({"key": "first", "dict_field.first_key": EnumTest.Value2})

    assert 0 == controller.rollback_to({"revision": 2})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
    ] == controller.get_history({})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        }
    ] == controller.get({})


def test_rollback_unknown_criteria_is_valid(controller):
    controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    before_update = 1
    controller.put({"key": "first", "dict_field.first_key": EnumTest.Value2})

    assert 0 == controller.rollback_to({"revision": before_update, "key": "unknown"})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
    ] == controller.get_history({})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        }
    ] == controller.get({})


def test_versioned_many(controller):
    controller.post_many(
        [
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            },
            {
                "key": "second",
                "dict_field.first_key": EnumTest.Value2,
                "dict_field.second_key": 2,
            },
        ]
    )
    controller.put_many(
        [
            {"key": "first", "dict_field.first_key": EnumTest.Value2},
            {"key": "second", "dict_field.second_key": 3},
        ]
    )

    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
        {
            "key": "second",
            "dict_field": {"first_key": "Value2", "second_key": 3},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
        {
            "key": "second",
            "dict_field": {"first_key": "Value2", "second_key": 2},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
    ] == controller.get_history({})


def test_rollback_without_revision_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.rollback_to({"key": "unknown"})
    assert {
        "revision": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "unknown"} == exception_info.value.received_data


def test_rollback_with_non_int_revision_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.rollback_to({"revision": "invalid revision"})
    assert {"revision": ["Not a valid int."]} == exception_info.value.errors
    assert {"revision": "invalid revision"} == exception_info.value.received_data


def test_rollback_with_negative_revision_is_valid(controller):
    assert 0 == controller.rollback_to({"revision": -1})


def test_rollback_before_existing_is_valid(controller):
    controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    before_insert = 1
    controller.post(
        {
            "key": "second",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert 1 == controller.rollback_to({"revision": before_insert})
    assert [] == controller.get({"key": "second"})


def test_get_revision_is_valid_when_empty(controller):
    assert 0 == controller._model.current_revision()


def test_get_revision_is_valid_when_1(controller):
    controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert 1 == controller._model.current_revision()


def test_get_revision_is_valid_when_2(controller):
    controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    controller.post(
        {
            "key": "second",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert 2 == controller._model.current_revision()


def test_rollback_to_0(controller):
    controller.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    controller.post(
        {
            "key": "second",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert 2 == controller.rollback_to({"revision": 0})
    assert [] == controller.get({})


def test_rollback_multiple_rows_is_valid(controller):
    controller.post(
        {
            "key": "1",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    controller.post(
        {
            "key": "2",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    controller.put({"key": "1", "dict_field.first_key": EnumTest.Value2})
    controller.delete({"key": "2"})
    controller.post(
        {
            "key": "3",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    controller.post(
        {
            "key": "4",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    before_insert = 6
    controller.post(
        {
            "key": "5",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    controller.put({"key": "1", "dict_field.second_key": 2})
    # Remove key 5 and Update key 1 (Key 3 and Key 4 unchanged)
    assert 2 == controller.rollback_to({"revision": before_insert})
    assert [
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "3",
            "valid_since_revision": 5,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "4",
            "valid_since_revision": 6,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "key": "1",
            "valid_since_revision": 9,
            "valid_until_revision": -1,
        },
    ] == controller.get({})
    assert [
        {
            "dict_field": {"first_key": "Value2", "second_key": 2},
            "key": "1",
            "valid_since_revision": 8,
            "valid_until_revision": 9,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "2",
            "valid_since_revision": 2,
            "valid_until_revision": 4,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "1",
            "valid_since_revision": 1,
            "valid_until_revision": 3,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "3",
            "valid_since_revision": 5,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "4",
            "valid_since_revision": 6,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "5",
            "valid_since_revision": 7,
            "valid_until_revision": 9,
        },
        {
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "key": "1",
            "valid_since_revision": 3,
            "valid_until_revision": 8,
        },
        {
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "key": "1",
            "valid_since_revision": 9,
            "valid_until_revision": -1,
        },
    ] == controller.get_history({})


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
                                "$ref": "#/definitions/TestModel_PostRequestModel"
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
                                "$ref": "#/definitions/TestModel_PutRequestModel"
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
                            "name": "dict_field.first_key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field.second_key",
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
                            "name": "dict_field.first_key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field.second_key",
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
                    ],
                    "tags": ["Test"],
                },
            },
            "/test_rollback_parser": {
                "get": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "get_test_rollback_parser_resource",
                    "parameters": [
                        {
                            "name": "dict_field.first_key",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "dict_field.second_key",
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
        },
        "info": {"title": "API", "version": "1.0"},
        "produces": ["application/json"],
        "consumes": ["application/json"],
        "tags": [{"name": "Test"}],
        "definitions": {
            "TestModel_PostRequestModel": {
                "required": ["dict_field"],
                "properties": {
                    "dict_field": {
                        "readOnly": False,
                        "default": {"first_key": None, "second_key": None},
                        "example": {"first_key": "Value1", "second_key": 1},
                        "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                },
                "type": "object",
            },
            "first_key_second_key": {
                "properties": {
                    "first_key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "Value1",
                        "enum": ["Value1", "Value2"],
                    },
                    "second_key": {"type": "integer", "readOnly": False, "example": 1},
                },
                "type": "object",
            },
            "TestModel_PutRequestModel": {
                "required": ["dict_field"],
                "properties": {
                    "dict_field": {
                        "readOnly": False,
                        "default": {"first_key": None, "second_key": None},
                        "example": {"first_key": "Value1", "second_key": 1},
                        "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                },
                "type": "object",
            },
            "TestModel_GetResponseModel": {
                "required": ["dict_field"],
                "properties": {
                    "dict_field": {
                        "readOnly": False,
                        "default": {"first_key": None, "second_key": None},
                        "example": {"first_key": "Value1", "second_key": 1},
                        "allOf": [{"$ref": "#/definitions/first_key_second_key"}],
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
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


def test_query_rollback_parser(client):
    response = client.get(
        "/test_rollback_parser?dict_field.first_key=4&dict_field.second_key=3&key=test&revision=2"
    )
    assert response.json == {
        "dict_field.first_key": ["4"],
        "dict_field.second_key": [3],
        "key": ["test"],
        "revision": 2,
    }
