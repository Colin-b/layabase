import datetime
import collections.abc

import flask
import flask_restx
import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        int_value = layabase.mongo.Column(int, allow_comparison_signs=True)
        float_value = layabase.mongo.Column(float, allow_comparison_signs=True)
        date_value = layabase.mongo.Column(datetime.date, allow_comparison_signs=True)
        datetime_value = layabase.mongo.Column(
            datetime.datetime, allow_comparison_signs=True
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


@pytest.fixture
def app(controller: layabase.CRUDController):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restx.Api(application)
    namespace = api.namespace("Test", path="/")

    controller.flask_restx.init_models(namespace)

    @namespace.route("/test")
    class TestResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_get_parser)
        @namespace.marshal_with(controller.flask_restx.get_response_model)
        def get(self):
            return []

        @namespace.expect(controller.flask_restx.json_post_model)
        def post(self):
            return []

        @namespace.expect(controller.flask_restx.json_put_model)
        def put(self):
            return []

        @namespace.expect(controller.flask_restx.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_get_parser)
        def get(self):
            return {
                field: [
                    str(value)
                    if isinstance(value, datetime.date) or isinstance(value, tuple)
                    else value
                    for value in values
                ]
                if isinstance(values, collections.abc.Iterable)
                else values
                for field, values in controller.flask_restx.query_get_parser.parse_args().items()
            }

        @namespace.expect(controller.flask_restx.query_delete_parser)
        def delete(self):
            return {
                field: [
                    str(value)
                    if isinstance(value, datetime.date) or isinstance(value, tuple)
                    else value
                    for value in values
                ]
                if isinstance(values, collections.abc.Iterable)
                else values
                for field, values in controller.flask_restx.query_delete_parser.parse_args().items()
            }

    return application


def test_query_with_int_and_less_than_sign_in_int_column_returns_tuple(client):
    response = client.get("/test_parsers?int_value=<1")
    assert response.json == {
        "date_value": None,
        "datetime_value": None,
        "float_value": None,
        "int_value": ["(<ComparisonSigns.Lower: '<'>, 1)"],
        "limit": None,
        "offset": None,
    }


def test_query_with_int_and_greater_than_sign_in_int_column_returns_tuple(client):
    response = client.get("/test_parsers?int_value=>1")
    assert response.json == {
        "date_value": None,
        "datetime_value": None,
        "float_value": None,
        "int_value": ["(<ComparisonSigns.Greater: '>'>, 1)"],
        "limit": None,
        "offset": None,
    }


def test_query_with_int_and_less_than_or_equal_sign_in_int_column_returns_tuple(client):
    response = client.get("/test_parsers?int_value=<=1")
    assert response.json == {
        "date_value": None,
        "datetime_value": None,
        "float_value": None,
        "int_value": ["(<ComparisonSigns.LowerOrEqual: '<='>, 1)"],
        "limit": None,
        "offset": None,
    }


def test_query_with_int_and_greater_than_or_equal_sign_in_int_column_returns_tuple(
    client,
):
    response = client.get("/test_parsers?int_value=>=1")
    assert response.json == {
        "date_value": None,
        "datetime_value": None,
        "float_value": None,
        "int_value": ["(<ComparisonSigns.GreaterOrEqual: '>='>, 1)"],
        "limit": None,
        "offset": None,
    }


def test_query_with_float_and_less_than_sign_in_float_column_returns_tuple(client):
    response = client.get("/test_parsers?float_value=<0.9")
    assert response.json == {
        "date_value": None,
        "datetime_value": None,
        "float_value": ["(<ComparisonSigns.Lower: '<'>, 0.9)"],
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_float_and_greater_than_sign_in_float_column_returns_tuple(client):
    response = client.get("/test_parsers?float_value=>0.9")
    assert response.json == {
        "date_value": None,
        "datetime_value": None,
        "float_value": ["(<ComparisonSigns.Greater: '>'>, 0.9)"],
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_float_and_less_than_or_equal_sign_in_float_column_returns_tuple(
    client,
):
    response = client.get("/test_parsers?float_value=<=0.9")
    assert response.json == {
        "date_value": None,
        "datetime_value": None,
        "float_value": ["(<ComparisonSigns.LowerOrEqual: '<='>, 0.9)"],
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_float_and_greater_than_or_equal_sign_in_float_column_returns_tuple(
    client,
):
    response = client.get("/test_parsers?float_value=>=0.9")
    assert response.json == {
        "date_value": None,
        "datetime_value": None,
        "float_value": ["(<ComparisonSigns.GreaterOrEqual: '>='>, 0.9)"],
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_date_and_less_than_sign_in_date_column_returns_tuple(client):
    response = client.get("/test_parsers?date_value=<2019-01-01")
    assert response.json == {
        "date_value": ["(<ComparisonSigns.Lower: '<'>, datetime.date(2019, 1, 1))"],
        "datetime_value": None,
        "float_value": None,
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_date_and_greater_than_sign_in_date_column_returns_tuple(client):
    response = client.get("/test_parsers?date_value=>2019-01-01")
    assert response.json == {
        "date_value": ["(<ComparisonSigns.Greater: '>'>, datetime.date(2019, 1, 1))"],
        "datetime_value": None,
        "float_value": None,
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_date_and_less_than_or_equal_sign_in_date_column_returns_tuple(
    client,
):
    response = client.get("/test_parsers?date_value=<=2019-01-01")
    assert response.json == {
        "date_value": [
            "(<ComparisonSigns.LowerOrEqual: '<='>, " "datetime.date(2019, 1, 1))"
        ],
        "datetime_value": None,
        "float_value": None,
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_date_and_greater_than_or_equal_sign_in_date_column_returns_tuple(
    client,
):
    response = client.get("/test_parsers?date_value=>=2019-01-01")
    assert response.json == {
        "date_value": [
            "(<ComparisonSigns.GreaterOrEqual: '>='>, " "datetime.date(2019, 1, 1))"
        ],
        "datetime_value": None,
        "float_value": None,
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_datetime_and_less_than_sign_in_datetime_column_returns_tuple(
    client,
):
    response = client.get("/test_parsers?datetime_value=<2019-01-02T23:59:59")
    assert response.json == {
        "date_value": None,
        "datetime_value": [
            "(<ComparisonSigns.Lower: '<'>, datetime.datetime(2019, 1, "
            "2, 23, 59, 59, tzinfo=datetime.timezone.utc))"
        ],
        "float_value": None,
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_datetime_and_greater_than_sign_in_datetime_column_returns_tuple(
    client,
):
    response = client.get("/test_parsers?datetime_value=>2019-01-02T23:59:59")
    assert response.json == {
        "date_value": None,
        "datetime_value": [
            "(<ComparisonSigns.Greater: '>'>, datetime.datetime(2019, "
            "1, 2, 23, 59, 59, tzinfo=datetime.timezone.utc))"
        ],
        "float_value": None,
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_datetime_and_less_than_or_equal_sign_in_datetime_column_returns_tuple(
    client,
):
    response = client.get("/test_parsers?datetime_value=<=2019-01-02T23:59:59")
    assert response.json == {
        "date_value": None,
        "datetime_value": [
            "(<ComparisonSigns.LowerOrEqual: '<='>, "
            "datetime.datetime(2019, 1, 2, 23, 59, 59, "
            "tzinfo=datetime.timezone.utc))"
        ],
        "float_value": None,
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_datetime_and_greater_than_or_equal_sign_in_datetime_column_returns_tuple(
    client,
):
    response = client.get("/test_parsers?datetime_value=>=2019-01-02T23:59:59")
    assert response.json == {
        "date_value": None,
        "datetime_value": [
            "(<ComparisonSigns.GreaterOrEqual: '>='>, "
            "datetime.datetime(2019, 1, 2, 23, 59, 59, "
            "tzinfo=datetime.timezone.utc))"
        ],
        "float_value": None,
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_int_range_using_comparison_signs_in_int_column_returns_list_of_tuples(
    client,
):
    response = client.get("/test_parsers?int_value=>=122&int_value=<124")
    assert response.json == {
        "date_value": None,
        "datetime_value": None,
        "float_value": None,
        "int_value": [
            "(<ComparisonSigns.GreaterOrEqual: '>='>, 122)",
            "(<ComparisonSigns.Lower: '<'>, 124)",
        ],
        "limit": None,
        "offset": None,
    }


def test_query_with_float_range_using_comparison_signs_in_float_column_returns_list_of_tuples(
    client,
):
    response = client.get("/test_parsers?float_value=>0.9&float_value=<=1.1")
    assert response.json == {
        "date_value": None,
        "datetime_value": None,
        "float_value": [
            "(<ComparisonSigns.Greater: '>'>, 0.9)",
            "(<ComparisonSigns.LowerOrEqual: '<='>, 1.1)",
        ],
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_date_range_using_comparison_signs_in_date_column_returns_list_of_tuples(
    client,
):
    response = client.get("/test_parsers?date_value=>2019-01-01&date_value=<2019-01-03")
    assert response.json == {
        "date_value": [
            "(<ComparisonSigns.Greater: '>'>, datetime.date(2019, 1, 1))",
            "(<ComparisonSigns.Lower: '<'>, datetime.date(2019, 1, 3))",
        ],
        "datetime_value": None,
        "float_value": None,
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_datetime_range_using_comparison_signs_in_datetime_column_returns_list_of_tuples(
    client,
):
    response = client.get(
        "/test_parsers?datetime_value=>=2019-01-01T23:59:59&datetime_value=<=2019-01-03T23:59:59"
    )
    assert response.json == {
        "date_value": None,
        "datetime_value": [
            "(<ComparisonSigns.GreaterOrEqual: '>='>, "
            "datetime.datetime(2019, 1, 1, 23, 59, 59, "
            "tzinfo=datetime.timezone.utc))",
            "(<ComparisonSigns.LowerOrEqual: '<='>, "
            "datetime.datetime(2019, 1, 3, 23, 59, 59, "
            "tzinfo=datetime.timezone.utc))",
        ],
        "float_value": None,
        "int_value": None,
        "limit": None,
        "offset": None,
    }


def test_query_with_int_range_and_value_out_of_range_using_comparison_signs_in_int_column_returns_list_of_tuples_and_values(
    client,
):
    response = client.get("/test_parsers?int_value=>=122&int_value=<124&int_value=125")
    assert response.json == {
        "date_value": None,
        "datetime_value": None,
        "float_value": None,
        "int_value": [
            "(<ComparisonSigns.GreaterOrEqual: '>='>, 122)",
            "(<ComparisonSigns.Lower: '<'>, 124)",
            125,
        ],
        "limit": None,
        "offset": None,
    }


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
                                "$ref": "#/definitions/TestCollection_PostRequestModel"
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
                                "$ref": "#/definitions/TestCollection_PutRequestModel"
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
                            "name": "int_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "float_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "date_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "datetime_value",
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
                                "$ref": "#/definitions/TestCollection_GetResponseModel"
                            },
                        }
                    },
                    "operationId": "get_test_resource",
                    "parameters": [
                        {
                            "name": "int_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "float_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "date_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "datetime_value",
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
            },
            "/test_parsers": {
                "delete": {
                    "responses": {"200": {"description": "Success"}},
                    "operationId": "delete_test_parsers_resource",
                    "parameters": [
                        {
                            "name": "int_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "float_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "date_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "datetime_value",
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
                            "name": "int_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "float_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "date_value",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "string"},
                            "collectionFormat": "multi",
                        },
                        {
                            "name": "datetime_value",
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
            "TestCollection_PostRequestModel": {
                "properties": {
                    "date_value": {
                        "type": "string",
                        "format": "date",
                        "readOnly": False,
                        "example": "2017-09-24",
                    },
                    "datetime_value": {
                        "type": "string",
                        "format": "date-time",
                        "readOnly": False,
                        "example": "2017-09-24T15:36:09",
                    },
                    "float_value": {
                        "type": "number",
                        "readOnly": False,
                        "example": 1.4,
                    },
                    "int_value": {"type": "integer", "readOnly": False, "example": 1},
                },
                "type": "object",
            },
            "TestCollection_PutRequestModel": {
                "properties": {
                    "date_value": {
                        "type": "string",
                        "format": "date",
                        "readOnly": False,
                        "example": "2017-09-24",
                    },
                    "datetime_value": {
                        "type": "string",
                        "format": "date-time",
                        "readOnly": False,
                        "example": "2017-09-24T15:36:09",
                    },
                    "float_value": {
                        "type": "number",
                        "readOnly": False,
                        "example": 1.4,
                    },
                    "int_value": {"type": "integer", "readOnly": False, "example": 1},
                },
                "type": "object",
            },
            "TestCollection_GetResponseModel": {
                "properties": {
                    "date_value": {
                        "type": "string",
                        "format": "date",
                        "readOnly": False,
                        "example": "2017-09-24",
                    },
                    "datetime_value": {
                        "type": "string",
                        "format": "date-time",
                        "readOnly": False,
                        "example": "2017-09-24T15:36:09",
                    },
                    "float_value": {
                        "type": "number",
                        "readOnly": False,
                        "example": 1.4,
                    },
                    "int_value": {"type": "integer", "readOnly": False, "example": 1},
                },
                "type": "object",
            },
        },
        "responses": {
            "ParseError": {"description": "When a mask can't be parsed"},
            "MaskError": {"description": "When any error occurs on mask"},
        },
    }


def test_query_get_parser_without_signs(client):
    response = client.get(
        "/test_parsers?date_value=2019-02-25&datetime_value=2019-02-25T15:56:59&float_value=2.5&int_value=15&limit=1&offset=0"
    )
    assert response.json == {
        "date_value": ["2019-02-25"],
        "datetime_value": ["2019-02-25 15:56:59+00:00"],
        "float_value": [2.5],
        "int_value": [15],
        "limit": 1,
        "offset": 0,
    }


def test_query_delete_parser_without_signs(client):
    response = client.delete(
        "/test_parsers?date_value=2019-02-25&datetime_value=2019-02-25T15:56:59&float_value=2.5&int_value=15"
    )
    assert response.json == {
        "date_value": ["2019-02-25"],
        "datetime_value": ["2019-02-25 15:56:59+00:00"],
        "float_value": [2.5],
        "int_value": [15],
    }


def test_query_get_parser_with_signs(client):
    response = client.get(
        "/test_parsers?date_value=>=2019-02-25&datetime_value=<=2019-02-25T15:56:59&float_value=>2.5&int_value=<15&limit=1&offset=0"
    )
    assert response.json == {
        "date_value": [
            "(<ComparisonSigns.GreaterOrEqual: '>='>, " "datetime.date(2019, 2, 25))"
        ],
        "datetime_value": [
            "(<ComparisonSigns.LowerOrEqual: '<='>, "
            "datetime.datetime(2019, 2, 25, 15, 56, 59, "
            "tzinfo=datetime.timezone.utc))"
        ],
        "float_value": ["(<ComparisonSigns.Greater: '>'>, 2.5)"],
        "int_value": ["(<ComparisonSigns.Lower: '<'>, 15)"],
        "limit": 1,
        "offset": 0,
    }


def test_query_delete_parser_with_signs(client):
    response = client.delete(
        "/test_parsers?date_value=>=2019-02-25&datetime_value=<=2019-02-25T15:56:59&float_value=>2.5&int_value=<15"
    )
    assert response.json == {
        "date_value": [
            "(<ComparisonSigns.GreaterOrEqual: '>='>, " "datetime.date(2019, 2, 25))"
        ],
        "datetime_value": [
            "(<ComparisonSigns.LowerOrEqual: '<='>, "
            "datetime.datetime(2019, 2, 25, 15, 56, 59, "
            "tzinfo=datetime.timezone.utc))"
        ],
        "float_value": ["(<ComparisonSigns.Greater: '>'>, 2.5)"],
        "int_value": ["(<ComparisonSigns.Lower: '<'>, 15)"],
    }
