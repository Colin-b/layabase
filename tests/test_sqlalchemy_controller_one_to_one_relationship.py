import flask
import flask_restplus
import pytest
import sqlalchemy
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship

import layabase


@pytest.fixture
def controller1(controller2):
    class TestTable1:
        __tablename__ = "test1"

        id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

        @declared_attr
        def other_id(cls):
            return sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey("test2.id"), primary_key=True)

        @declared_attr
        def test2(cls):
            # https://docs.sqlalchemy.org/en/13/orm/loading_relationships.html
            return relationship("TestTable2_SQLAlchemyModel", innerjoin=True, lazy="joined", uselist=False)

        # TODO Put this logic in the code to find XXXX.YYY on model XXXX and remove this ugly way of overriding field retrieval
        @classmethod
        def get_column(cls, field_name: str):
            if "test2.other_info" == field_name:
                return controller2._model.other_info
            return super().get_column(field_name)

    return layabase.CRUDController(TestTable1)

# cls._session.query(cls).options(subqueryload(cls.test2)).filter(cls.test2.mapper.columns["other_info"].in_(["test1"])).all()

@pytest.fixture
def controller2():
    class TestTable2:
        __tablename__ = "test2"

        id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
        other_info = sqlalchemy.Column(sqlalchemy.String)

    return layabase.CRUDController(TestTable2)


@pytest.fixture
def controllers(controller1, controller2):
    base = layabase.load("sqlite:///:memory:", [controller2, controller1])
    controller2.post_many([
        {"id": 10, "other_info": "test1"},
        {"id": 20, "other_info": "test2"},
        {"id": 30, "other_info": "test2"},
    ])
    controller1.post_many([
        {"id": 1, "other_id": 10},
        {"id": 1, "other_id": 20},
        {"id": 1, "other_id": 30},
    ])
    return base


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

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        @namespace.expect(controller1.query_get_parser)
        def get(self):
            all = controller1.get({"test2.other_info": "test1"})
            return controller1.query_get_parser.parse_args()

        @namespace.expect(controller1.query_delete_parser)
        def delete(self):
            return controller1.query_delete_parser.parse_args()

    return application


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
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
                            "name": "bool_field",
                            "in": "query",
                            "type": "array",
                            "items": {"type": "boolean"},
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
            "TestTable_PostRequestModel": {
                "required": ["key"],
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample_value",
                    },
                    "bool_field": {
                        "type": "boolean",
                        "readOnly": False,
                        "example": True,
                    },
                },
                "type": "object",
            },
            "TestTable_PutRequestModel": {
                "required": ["key"],
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample_value",
                    },
                    "bool_field": {
                        "type": "boolean",
                        "readOnly": False,
                        "example": True,
                    },
                },
                "type": "object",
            },
            "TestTable_GetResponseModel": {
                "required": ["key"],
                "properties": {
                    "key": {
                        "type": "string",
                        "readOnly": False,
                        "example": "sample_value",
                    },
                    "bool_field": {
                        "type": "boolean",
                        "readOnly": False,
                        "example": True,
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


def test_query_get_parser(client):
    response = client.get(
        "/test_parsers?key=12&bool_field=true&limit=1&order_by=key&offset=0"
    )
    assert response.json == {
        "bool_field": [True],
        "key": ["12"],
        "limit": 1,
        "offset": 0,
        "order_by": ["key"],
    }


def test_query_delete_parser(client):
    response = client.delete("/test_parsers?key=12&bool_field=true")
    assert response.json == {"bool_field": [True], "key": ["12"]}
