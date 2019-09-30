import flask
import flask_restplus
import pytest
import sqlalchemy

import layabase
import layabase.testing


class TestTimeController(layabase.CRUDController):
    class TestTimeModel:
        __tablename__ = "time_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        time_field = sqlalchemy.Column(sqlalchemy.Time)

    model = TestTimeModel


@pytest.fixture
def db():
    _db = layabase.load("sqlite:///:memory:", [TestTimeController])
    yield _db
    layabase.testing.reset(_db)


@pytest.fixture
def app(db):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    TestTimeController.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(TestTimeController.query_get_parser)
        @namespace.marshal_with(TestTimeController.get_response_model)
        def get(self):
            return []

        @namespace.expect(TestTimeController.json_post_model)
        def post(self):
            return []

        @namespace.expect(TestTimeController.json_put_model)
        def put(self):
            return []

        @namespace.expect(TestTimeController.query_delete_parser)
        def delete(self):
            return []

    return application


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
                                "$ref": "#/definitions/TestTimeModel_GetResponseModel"
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
                            "name": "time_field",
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
                            "schema": {
                                "$ref": "#/definitions/TestTimeModel_PostRequestModel"
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
                                "$ref": "#/definitions/TestTimeModel_PutRequestModel"
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
                            "name": "time_field",
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
            "TestTimeModel_PostRequestModel": {
                "required": ["key"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "time_field": {
                        "type": "string",
                        "format": "date-time",
                        "example": "15:36:09",
                    },
                },
                "type": "object",
            },
            "TestTimeModel_PutRequestModel": {
                "required": ["key"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "time_field": {
                        "type": "string",
                        "format": "date-time",
                        "example": "15:36:09",
                    },
                },
                "type": "object",
            },
            "TestTimeModel_GetResponseModel": {
                "required": ["key"],
                "properties": {
                    "key": {"type": "string", "example": "sample_value"},
                    "time_field": {
                        "type": "string",
                        "format": "date-time",
                        "example": "15:36:09",
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
