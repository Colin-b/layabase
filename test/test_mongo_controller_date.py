import datetime

import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed, ModelCouldNotBeFound

from layabase import database, database_mongo


class TestDateController(database.CRUDController):
    pass


def _create_models(base):
    class TestDateModel(
        database_mongo.CRUDModel, base=base, table_name="date_table_name"
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        date_str = database_mongo.Column(datetime.date)
        datetime_str = database_mongo.Column(datetime.datetime)

    TestDateController.model(TestDateModel)

    return [TestDateModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    yield _db
    database.reset(_db)


@pytest.fixture
def app(db):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    TestDateController.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(TestDateController.query_get_parser)
        @namespace.marshal_with(TestDateController.get_response_model)
        def get(self):
            return []

        @namespace.expect(TestDateController.json_post_model)
        def post(self):
            return []

        @namespace.expect(TestDateController.json_put_model)
        def put(self):
            return []

        @namespace.expect(TestDateController.query_delete_parser)
        def delete(self):
            return []

    return application


def test_put_is_updating_date(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert (
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        },
        {
            "date_str": "2018-06-01",
            "datetime_str": "1989-12-31T01:00:00",
            "key": "my_key1",
        },
    ) == TestDateController.put(
        {
            "key": "my_key1",
            "date_str": "2018-06-01",
            "datetime_str": "1989-12-31T01:00:00",
        }
    )
    assert [
        {
            "date_str": "2018-06-01",
            "datetime_str": "1989-12-31T01:00:00",
            "key": "my_key1",
        }
    ] == TestDateController.get({"date_str": "2018-06-01"})


def test_get_date_is_handled_for_valid_date(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    d = datetime.datetime.strptime("2017-05-15", "%Y-%m-%d").date()
    assert [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ] == TestDateController.get({"date_str": d})


def test_post_invalid_date_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "this is not a date",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
    assert {"date_str": ["Not a valid date."]} == exception_info.value.errors
    assert {
        "key": "my_key1",
        "date_str": "this is not a date",
        "datetime_str": "2016-09-23T23:59:59",
    } == exception_info.value.received_data


def test_get_invalid_date_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDateController.get({"date_str": "this is not a date"})
    assert {"date_str": ["Not a valid date."]} == exception_info.value.errors
    assert {"date_str": "this is not a date"} == exception_info.value.received_data


def test_delete_invalid_date_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDateController.delete({"date_str": "this is not a date"})
    assert {"date_str": ["Not a valid date."]} == exception_info.value.errors
    assert {"date_str": "this is not a date"} == exception_info.value.received_data


def test_get_with_unknown_fields_is_valid(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert [
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    ] == TestDateController.get({"date_str": "2018-12-30", "unknown_field": "value"})


def test_put_with_unknown_fields_is_valid(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert (
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        },
        {
            "key": "my_key1",
            "date_str": "2018-12-31",
            "datetime_str": "2016-09-23T23:59:59",
        },
    ) == TestDateController.put(
        {"key": "my_key1", "date_str": "2018-12-31", "unknown_field": "value"}
    )
    assert [
        {
            "key": "my_key1",
            "date_str": "2018-12-31",
            "datetime_str": "2016-09-23T23:59:59",
        }
    ] == TestDateController.get({"date_str": "2018-12-31"})
    assert [] == TestDateController.get({"date_str": "2018-12-30"})


def test_put_unexisting_is_invalid(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    with pytest.raises(ModelCouldNotBeFound) as exception_info:
        TestDateController.put({"key": "my_key2"})
    assert {"key": "my_key2"} == exception_info.value.requested_data


def test_post_invalid_datetime_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "2016-09-23",
                "datetime_str": "This is not a valid datetime",
            }
        )
    assert {"datetime_str": ["Not a valid datetime."]} == exception_info.value.errors
    assert {
        "key": "my_key1",
        "date_str": "2016-09-23",
        "datetime_str": "This is not a valid datetime",
    } == exception_info.value.received_data


def test_post_datetime_for_a_date_is_valid(db):
    assert {
        "key": "my_key1",
        "date_str": "2017-05-01",
        "datetime_str": "2017-05-30T01:05:45",
    } == TestDateController.post(
        {
            "key": "my_key1",
            "date_str": datetime.datetime.strptime(
                "2017-05-01T01:05:45", "%Y-%m-%dT%H:%M:%S"
            ),
            "datetime_str": "2017-05-30T01:05:45",
        }
    )


def test_get_date_is_handled_for_unused_date(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    d = datetime.datetime.strptime("2016-09-23", "%Y-%m-%d").date()
    assert [] == TestDateController.get({"date_str": d})


def test_get_date_is_handled_for_valid_datetime(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    dt = datetime.datetime.strptime("2016-09-23T23:59:59", "%Y-%m-%dT%H:%M:%S")
    assert [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ] == TestDateController.get({"datetime_str": dt})


def test_get_date_is_handled_for_unused_datetime(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    dt = datetime.datetime.strptime("2016-09-24T23:59:59", "%Y-%m-%dT%H:%M:%S")
    assert [] == TestDateController.get({"datetime_str": dt})


def test_open_api_definition(client):
    response = client.get("/swagger.json")
    assert response.json == {
        "basePath": "/",
        "consumes": ["application/json"],
        "definitions": {
            "TestDateModel": {
                "properties": {
                    "date_str": {
                        "example": "2017-09-24",
                        "format": "date",
                        "readOnly": False,
                        "type": "string",
                    },
                    "datetime_str": {
                        "example": "2017-09-24T15:36:09",
                        "format": "date-time",
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
            }
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
                            "items": {"type": "array"},
                            "name": "date_str",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "datetime_str",
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
                            "items": {"type": "array"},
                            "name": "date_str",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "array"},
                            "name": "datetime_str",
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
                            "schema": {"$ref": "#/definitions/TestDateModel"},
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
                            "schema": {"$ref": "#/definitions/TestDateModel"},
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
                            "schema": {"$ref": "#/definitions/TestDateModel"},
                        }
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["Test"],
                },
            }
        },
        "produces": ["application/json"],
        "responses": {
            "MaskError": {"description": "When any error occurs on mask"},
            "ParseError": {"description": "When a mask can't be parsed"},
        },
        "swagger": "2.0",
        "tags": [{"name": "Test"}],
    }
