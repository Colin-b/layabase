import datetime

import flask
import flask_restplus
import pytest
from layaberr import ValidationFailed, ModelCouldNotBeFound

import layabase
import layabase.database_mongo
import layabase.testing


@pytest.fixture
def controller():
    class TestModel:
        __tablename__ = "test"

        key = layabase.database_mongo.Column(str, is_primary_key=True)
        date_str = layabase.database_mongo.Column(datetime.date)
        datetime_str = layabase.database_mongo.Column(datetime.datetime)

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

    return application


def test_put_is_updating_date(controller):
    controller.post(
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
    ) == controller.put(
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
    ] == controller.get({"date_str": "2018-06-01"})


def test_get_date_is_handled_for_valid_date(controller):
    controller.post(
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
    ] == controller.get({"date_str": d})


def test_post_invalid_date_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post(
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


def test_get_invalid_date_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.get({"date_str": "this is not a date"})
    assert {"date_str": ["Not a valid date."]} == exception_info.value.errors
    assert {"date_str": "this is not a date"} == exception_info.value.received_data


def test_delete_invalid_date_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.delete({"date_str": "this is not a date"})
    assert {"date_str": ["Not a valid date."]} == exception_info.value.errors
    assert {"date_str": "this is not a date"} == exception_info.value.received_data


def test_get_with_unknown_fields_is_valid(controller):
    controller.post(
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
    ] == controller.get({"date_str": "2018-12-30", "unknown_field": "value"})


def test_put_with_unknown_fields_is_valid(controller):
    controller.post(
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
    ) == controller.put(
        {"key": "my_key1", "date_str": "2018-12-31", "unknown_field": "value"}
    )
    assert [
        {
            "key": "my_key1",
            "date_str": "2018-12-31",
            "datetime_str": "2016-09-23T23:59:59",
        }
    ] == controller.get({"date_str": "2018-12-31"})
    assert [] == controller.get({"date_str": "2018-12-30"})


def test_put_unexisting_is_invalid(controller):
    controller.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    with pytest.raises(ModelCouldNotBeFound) as exception_info:
        controller.put({"key": "my_key2"})
    assert {"key": "my_key2"} == exception_info.value.requested_data


def test_post_invalid_datetime_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post(
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


def test_post_datetime_for_a_date_is_valid(controller):
    assert {
        "key": "my_key1",
        "date_str": "2017-05-01",
        "datetime_str": "2017-05-30T01:05:45",
    } == controller.post(
        {
            "key": "my_key1",
            "date_str": datetime.datetime.strptime(
                "2017-05-01T01:05:45", "%Y-%m-%dT%H:%M:%S"
            ),
            "datetime_str": "2017-05-30T01:05:45",
        }
    )


def test_get_date_is_handled_for_unused_date(controller):
    controller.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    d = datetime.datetime.strptime("2016-09-23", "%Y-%m-%d").date()
    assert [] == controller.get({"date_str": d})


def test_get_date_is_handled_for_valid_datetime(controller):
    controller.post(
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
    ] == controller.get({"datetime_str": dt})


def test_get_date_is_handled_for_unused_datetime(controller):
    controller.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    dt = datetime.datetime.strptime("2016-09-24T23:59:59", "%Y-%m-%dT%H:%M:%S")
    assert [] == controller.get({"datetime_str": dt})


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
                            "name": "date_str",
                            "in": "query",
                            "type": "array",
                            "format": "date",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "datetime_str",
                            "in": "query",
                            "type": "array",
                            "format": "date-time",
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
                "delete": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "delete_test_resource",
                    "parameters": [
                        {
                            "name": "date_str",
                            "in": "query",
                            "type": "array",
                            "format": "date",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "datetime_str",
                            "in": "query",
                            "type": "array",
                            "format": "date-time",
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
            }
        },
        "info": {"title": "API", "version": "1.0"},
        "produces": ["application/json"],
        "consumes": ["application/json"],
        "tags": [{"name": "Test"}],
        "definitions": {
            "TestModel_PostRequestModel": {
                "properties": {
                    "date_str": {
                        "type": "string",
                        "format": "date",
                        "readOnly": False,
                        "example": "2017-09-24",
                    },
                    "datetime_str": {
                        "type": "string",
                        "format": "date-time",
                        "readOnly": False,
                        "example": "2017-09-24T15:36:09",
                    },
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample key",
                    },
                },
                "type": "object",
            },
            "TestModel_PutRequestModel": {
                "properties": {
                    "date_str": {
                        "type": "string",
                        "format": "date",
                        "readOnly": False,
                        "example": "2017-09-24",
                    },
                    "datetime_str": {
                        "type": "string",
                        "format": "date-time",
                        "readOnly": False,
                        "example": "2017-09-24T15:36:09",
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
                "properties": {
                    "date_str": {
                        "type": "string",
                        "format": "date",
                        "readOnly": False,
                        "example": "2017-09-24",
                    },
                    "datetime_str": {
                        "type": "string",
                        "format": "date-time",
                        "readOnly": False,
                        "example": "2017-09-24T15:36:09",
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
