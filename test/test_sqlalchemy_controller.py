from threading import Thread

import pytest
import sqlalchemy
import flask
import flask_restplus
from layaberr import ValidationFailed

from layabase import database, database_sqlalchemy


class TestController(database.CRUDController):
    pass


def _create_models(base):
    class TestModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "sample_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    TestController.model(TestModel)
    return [TestModel]


@pytest.fixture
def db():
    _db = database.load("sqlite:///:memory:", _create_models)
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

    @namespace.route("/test/description")
    class TestDescriptionResource(flask_restplus.Resource):
        @namespace.marshal_with(TestController.get_model_description_response_model)
        def get(self):
            return {}

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        def get(self):
            return TestController.query_get_parser.parse_args()

        def delete(self):
            return TestController.query_delete_parser.parse_args()

    return application


def test_get_without_providing_a_dictionary(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.get("")
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert "" == exception_info.value.received_data


def test_get_one_without_providing_a_dictionary(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.get_one("")
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert "" == exception_info.value.received_data


def test_get_last_without_providing_a_dictionary(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.get_last("")
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert "" == exception_info.value.received_data


def test_get_history_without_providing_a_dictionary(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.get_history("")
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert "" == exception_info.value.received_data


def test_delete_without_providing_a_dictionary(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.delete("")
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert "" == exception_info.value.received_data


def test_rollback_without_providing_a_dictionary(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.rollback_to("")
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert "" == exception_info.value.received_data


def test_get_all_without_data_returns_empty_list(db):
    assert [] == TestController.get({})


def test_post_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_post_list_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_post_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_post_without_providing_a_dictionary(client):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post("fail")
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert "fail" == exception_info.value.received_data


def test_post_many_with_empty_list_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([])
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_get_audit_when_not_audited(db):
    assert [] == TestController.get_audit({})


def test_put_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_put_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_delete_without_nothing_do_not_fail(db):
    assert 0 == TestController.delete({})


def test_post_without_mandatory_field_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({"key": "my_key"})
    assert {
        "mandatory": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "my_key"} == exception_info.value.received_data


def test_post_many_without_mandatory_field_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([{"key": "my_key"}])
    assert {
        0: {"mandatory": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [{"key": "my_key"}] == exception_info.value.received_data


def test_post_without_key_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({"mandatory": 1})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {"mandatory": 1} == exception_info.value.received_data


def test_post_many_without_key_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([{"mandatory": 1}])
    assert {
        0: {"key": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [{"mandatory": 1}] == exception_info.value.received_data


def test_post_with_wrong_type_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({"key": 256, "mandatory": 1})
    assert {"key": ["Not a valid string."]} == exception_info.value.errors
    assert {"key": 256, "mandatory": 1} == exception_info.value.received_data


def test_post_many_with_wrong_type_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([{"key": 256, "mandatory": 1}])
    assert {0: {"key": ["Not a valid string."]}} == exception_info.value.errors
    assert [{"key": 256, "mandatory": 1}] == exception_info.value.received_data


def test_put_with_wrong_type_is_invalid(db):
    TestController.post({"key": "value1", "mandatory": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put({"key": "value1", "mandatory": "invalid value"})
    assert {"mandatory": ["Not a valid integer."]} == exception_info.value.errors
    assert {
        "key": "value1",
        "mandatory": "invalid value",
    } == exception_info.value.received_data


def test_post_without_optional_is_valid(db):
    assert {"mandatory": 1, "key": "my_key", "optional": None} == TestController.post(
        {"key": "my_key", "mandatory": 1}
    )


def test_post_many_without_optional_is_valid(db):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": None},
        {"mandatory": 2, "key": "my_key2", "optional": None},
    ] == TestController.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )


def test_get_no_like_operator(db):
    TestController.post_many(
        [
            {"key": "my_key", "mandatory": 1},
            {"key": "my_key2", "mandatory": 1},
            {"key": "my_ey", "mandatory": 1},
            {"key": "my_k", "mandatory": 1},
            {"key": "y_key", "mandatory": 1},
        ]
    )
    assert [] == TestController.get({"key": "*y_k*"})


def test_post_with_optional_is_valid(db):
    assert {
        "mandatory": 1,
        "key": "my_key",
        "optional": "my_value",
    } == TestController.post({"key": "my_key", "mandatory": 1, "optional": "my_value"})


def test_post_many_with_optional_is_valid(db):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": "my_value"},
        {"mandatory": 2, "key": "my_key2", "optional": "my_value2"},
    ] == TestController.post_many(
        [
            {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )


def test_post_with_unknown_field_is_valid(db):
    assert {
        "mandatory": 1,
        "key": "my_key",
        "optional": "my_value",
    } == TestController.post(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    )


def test_post_many_with_unknown_field_is_valid(db):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": "my_value"},
        {"mandatory": 2, "key": "my_key2", "optional": "my_value2"},
    ] == TestController.post_many(
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
    )


def test_get_without_filter_is_retrieving_the_only_item(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert [
        {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
    ] == TestController.get({})


def test_get_from_another_thread_than_post(db):
    def save_get_result():
        assert [
            {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
        ] == TestController.get({})

    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})

    get_thread = Thread(name="GetInOtherThread", target=save_get_result)
    get_thread.start()
    get_thread.join()


def test_get_without_filter_is_retrieving_everything_with_multiple_posts(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({})


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


def test_get_with_filter_is_retrieving_subset_with_multiple_posts(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == TestController.get({"optional": "my_value1"})


def test_get_with_list_filter_matching_one_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == TestController.get({"optional": ["my_value1"]})


def test_get_with_list_filter_matching_many_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({"optional": ["my_value1", "my_value2"]})


def test_get_with_list_filter_matching_partial_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == TestController.get({"optional": ["non existing", "my_value1", "not existing"]})


def test_get_with_empty_list_filter_is_retrieving_everything(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({"optional": []})


def test_delete_with_list_filter_matching_one_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 1 == TestController.delete({"optional": ["my_value1"]})


def test_delete_with_list_filter_matching_many_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == TestController.delete({"optional": ["my_value1", "my_value2"]})


def test_delete_with_list_filter_matching_partial_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 1 == TestController.delete(
        {"optional": ["non existing", "my_value1", "not existing"]}
    )


def test_delete_with_empty_list_filter_is_retrieving_everything(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == TestController.delete({"optional": []})


def test_get_with_filter_is_retrieving_subset(db):
    TestController.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == TestController.get({"optional": "my_value1"})


def test_put_is_updating(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    ) == TestController.put({"key": "my_key1", "optional": "my_value"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ] == TestController.get({"mandatory": 1})


def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.put({"key": "my_key1", "optional": "my_value"})
    assert [] == TestController.get({"optional": "my_value1"})


def test_delete_with_filter_is_removing_the_proper_row(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 1 == TestController.delete({"key": "my_key1"})
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ] == TestController.get({})


def test_delete_without_filter_is_removing_everything(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == TestController.delete({})
    assert [] == TestController.get({})


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


def test_query_delete_parser(client):
    response = client.delete("/test_parsers?key=12&mandatory=123&optional=1234")
    assert response.json == {"key": ["12"], "mandatory": [123], "optional": ["1234"]}


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
            "TestModel": {
                "required": ["key", "mandatory"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "mandatory": {"type": "integer", "example": "0"},
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
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }


def test_get_with_order_by_desc_is_retrieving_elements_ordered_by_descending_mode(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert [
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
    ] == TestController.get({"order_by": ["key desc"]})


def test_get_with_order_by_is_retrieving_elements_ordered_by_ascending_mode(db):
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ] == TestController.get({"order_by": ["key"]})


def test_get_with_2_order_by_is_retrieving_elements_ordered_by(db):
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ] == TestController.get({"order_by": ["key", "mandatory desc"]})


def test_get_with_limit_2_is_retrieving_subset_of_2_first_elements(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({"limit": 2})


def test_get_with_offset_1_is_retrieving_subset_of_n_minus_1_first_elements(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ] == TestController.get({"offset": 1})


def test_get_with_limit_1_and_offset_1_is_retrieving_middle_element(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ] == TestController.get({"offset": 1, "limit": 1})


def test_get_model_description_returns_description(db):
    assert {
        "key": "key",
        "mandatory": "mandatory",
        "optional": "optional",
        "table": "sample_table_name",
    } == TestController.get_model_description()
