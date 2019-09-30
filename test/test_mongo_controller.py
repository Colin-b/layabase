import datetime
from threading import Thread
import logging
import sys

import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed

import layabase
import layabase.database_mongo
import layabase.testing

# Use debug logging to ensure that debug logging statements have no impact
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(process)d:%(thread)d - %(filename)s:%(lineno)d - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.DEBUG,
)


@pytest.fixture
def controller():
    class TestModel:
        __tablename__ = "sample_table_name"

        key = layabase.database_mongo.Column(str, is_primary_key=True)
        mandatory = layabase.database_mongo.Column(int, is_nullable=False)
        optional = layabase.database_mongo.Column(str)

    controller = layabase.CRUDController(TestModel)
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

    @namespace.route("/test/description")
    class TestDescriptionResource(flask_restplus.Resource):
        @namespace.marshal_with(controller.get_model_description_response_model)
        def get(self):
            return {}

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


def test_post_with_nothing_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert not exception_info.value.received_data


def test_post_list_with_nothing_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == []


def test_post_with_empty_dict_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({})
    assert exception_info.value.errors == {
        "key": ["Missing data for required field."],
        "mandatory": ["Missing data for required field."],
    }
    assert exception_info.value.received_data == {}


def test_post_with_list_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post([""])
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert [""] == exception_info.value.received_data


def test_post_many_with_dict_is_invalid(controller, client):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many({""})
    assert {"": ["Must be a list of dictionaries."]} == exception_info.value.errors
    assert {""} == exception_info.value.received_data


def test_put_with_list_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put([""])
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert [""] == exception_info.value.received_data


def test_put_many_with_dict_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put_many({""})
    assert {"": ["Must be a list."]} == exception_info.value.errors
    assert {""} == exception_info.value.received_data


def test_post_many_with_empty_list_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many([])
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert [] == exception_info.value.received_data


def test_put_with_nothing_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert None == exception_info.value.received_data


def test_put_with_empty_dict_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put({})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_delete_without_nothing_do_not_fail(controller):
    assert 0 == controller.delete({})


def test_post_without_mandatory_field_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"key": "my_key"})
    assert {
        "mandatory": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "my_key"} == exception_info.value.received_data


def test_post_many_without_mandatory_field_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many([{"key": "my_key"}])
    assert {
        0: {"mandatory": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [{"key": "my_key"}] == exception_info.value.received_data


def test_post_without_key_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"mandatory": 1})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {"mandatory": 1} == exception_info.value.received_data


def test_post_many_without_key_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many([{"mandatory": 1}])
    assert {
        0: {"key": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [{"mandatory": 1}] == exception_info.value.received_data


def test_post_with_wrong_type_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"key": datetime.date(2007, 12, 5), "mandatory": 1})
    assert {"key": ["Not a valid str."]} == exception_info.value.errors
    assert {
        "key": datetime.date(2007, 12, 5),
        "mandatory": 1,
    } == exception_info.value.received_data


def test_post_int_instead_of_str_is_valid(controller):
    assert {"key": "3", "mandatory": 1, "optional": None} == controller.post(
        {"key": 3, "mandatory": 1}
    )


def test_post_boolean_instead_of_str_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"key": True, "mandatory": 1})
    assert {"key": ["Not a valid str."]} == exception_info.value.errors
    assert {"key": True, "mandatory": 1} == exception_info.value.received_data


def test_post_float_instead_of_str_is_valid(controller):
    assert {"key": "1.5", "mandatory": 1, "optional": None} == controller.post(
        {"key": 1.5, "mandatory": 1}
    )


def test_rollback_without_versioning_is_valid(controller):
    assert 0 == controller.rollback_to({"revision": "invalid revision"})


def test_post_many_with_wrong_type_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many([{"key": datetime.date(2007, 12, 5), "mandatory": 1}])
    assert {0: {"key": ["Not a valid str."]}} == exception_info.value.errors
    assert [
        {"key": datetime.date(2007, 12, 5), "mandatory": 1}
    ] == exception_info.value.received_data


def test_put_with_wrong_type_is_invalid(controller):
    controller.post({"key": "value1", "mandatory": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put({"key": "value1", "mandatory": "invalid value"})
    assert {"mandatory": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "key": "value1",
        "mandatory": "invalid value",
    } == exception_info.value.received_data


def test_put_with_optional_as_none_is_valid(controller):
    controller.post({"key": "value1", "mandatory": 1})
    controller.put({"key": "value1", "mandatory": 1, "optional": None})
    assert [{"mandatory": 1, "key": "value1", "optional": None}] == controller.get({})


def test_put_with_non_nullable_as_none_is_invalid(controller):
    controller.post({"key": "value1", "mandatory": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put({"key": "value1", "mandatory": None})
    assert {
        "mandatory": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "value1", "mandatory": None} == exception_info.value.received_data


def test_post_without_optional_is_valid(controller):
    assert {"mandatory": 1, "key": "my_key", "optional": None} == controller.post(
        {"key": "my_key", "mandatory": 1}
    )


def test_get_with_non_nullable_none_is_valid(controller):
    assert {"mandatory": 1, "key": "my_key", "optional": None} == controller.post(
        {"key": "my_key", "mandatory": 1}
    )
    assert [{"mandatory": 1, "key": "my_key", "optional": None}] == controller.get(
        {"key": "my_key", "mandatory": None}
    )


def test_post_many_without_optional_is_valid(controller):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": None},
        {"mandatory": 2, "key": "my_key2", "optional": None},
    ] == controller.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )


def test_put_many_is_valid(controller):
    controller.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )
    assert (
        [
            {"mandatory": 1, "key": "my_key", "optional": None},
            {"mandatory": 2, "key": "my_key2", "optional": None},
        ],
        [
            {"mandatory": 1, "key": "my_key", "optional": "test"},
            {"mandatory": 3, "key": "my_key2", "optional": None},
        ],
    ) == controller.put_many(
        [{"key": "my_key", "optional": "test"}, {"key": "my_key2", "mandatory": 3}]
    )


def test_post_with_optional_is_valid(controller):
    assert {"mandatory": 1, "key": "my_key", "optional": "my_value"} == controller.post(
        {"key": "my_key", "mandatory": 1, "optional": "my_value"}
    )


def test_post_many_with_optional_is_valid(controller):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": "my_value"},
        {"mandatory": 2, "key": "my_key2", "optional": "my_value2"},
    ] == controller.post_many(
        [
            {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )


def test_post_with_unknown_field_is_valid(controller):
    assert {"mandatory": 1, "key": "my_key", "optional": "my_value"} == controller.post(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    )


def test_post_many_with_unknown_field_is_valid(controller):
    assert controller.post_many(
        [
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            },
            {
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                # This field do not exists in schema
                "unknown": "my_value2",
            },
        ]
    ) == [
        {"mandatory": 1, "key": "my_key", "optional": "my_value"},
        {"mandatory": 2, "key": "my_key2", "optional": "my_value2"},
    ]


def test_get_without_filter_is_retrieving_the_only_item(controller):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller.get({}) == [
        {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
    ]


def test_get_from_another_thread_than_post(controller):
    def save_get_result():
        assert controller.get({}) == [
            {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
        ]

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})

    get_thread = Thread(name="GetInOtherThread", target=save_get_result)
    get_thread.start()
    get_thread.join()


def test_get_without_filter_is_retrieving_everything_with_multiple_posts(controller):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]


def test_get_without_filter_is_retrieving_everything(controller):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == controller.get({})


def test_get_with_filter_is_retrieving_subset_with_multiple_posts(controller):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == controller.get({"optional": "my_value1"})


def test_get_with_filter_is_retrieving_subset(controller):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == controller.get({"optional": "my_value1"})


def test_put_is_updating(controller):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    ) == controller.put({"key": "my_key1", "optional": "my_value"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ] == controller.get({"mandatory": 1})


def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(controller):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "optional": "my_value"})
    assert [] == controller.get({"optional": "my_value1"})


def test_delete_with_filter_is_removing_the_proper_row(controller):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 1 == controller.delete({"key": "my_key1"})
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ] == controller.get({})


def test_delete_without_filter_is_removing_everything(controller):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == controller.delete({})
    assert [] == controller.get({})


def test_query_get_parser(client):
    response = client.get("/test_parsers?key=1&mandatory=2&optional=3&limit=4&offset=5")
    assert response.json == {
        "key": ["1"],
        "limit": 4,
        "mandatory": [2],
        "offset": 5,
        "optional": ["3"],
    }


def test_query_delete_parser(client):
    response = client.delete("/test_parsers?key=1&mandatory=2&optional=3")
    assert response.json == {"key": ["1"], "mandatory": [2], "optional": ["3"]}


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
            },
            "/test/description": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Success",
                            "schema": {
                                "$ref": "#/definitions/TestModel_GetDescriptionResponseModel"
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
            "/test_parsers": {
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
            "TestModel_GetDescriptionResponseModel": {
                "required": ["collection"],
                "properties": {
                    "collection": {
                        "type": "string",
                        "description": "Collection name",
                        "example": "collection",
                    },
                    "key": {"type": "string", "example": "column"},
                    "mandatory": {"type": "string", "example": "column"},
                    "optional": {"type": "string", "example": "column"},
                },
                "type": "object",
            },
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }


def test_get_with_limit_2_is_retrieving_subset_of_2_first_elements(controller):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    controller.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == controller.get({"limit": 2})


def test_get_with_offset_1_is_retrieving_subset_of_n_minus_1_first_elements(controller):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    controller.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ] == controller.get({"offset": 1})


def test_get_with_limit_1_and_offset_1_is_retrieving_middle_element(controller):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    controller.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ] == controller.get({"offset": 1, "limit": 1})


def test_get_model_description_returns_description(controller):
    assert {
        "key": "key",
        "mandatory": "mandatory",
        "optional": "optional",
        "collection": "sample_table_name",
    } == controller.get_model_description()
