import enum

import flask
import flask_restx
import pytest

import layabase
import layabase.mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        dict_col = layabase.mongo.DictColumn(
            fields={
                "first_key": layabase.mongo.Column(EnumTest, is_nullable=False),
                "second_key": layabase.mongo.Column(int, is_nullable=False),
            },
            is_nullable=False,
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

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_get_parser)
        def get(self):
            return controller.flask_restx.query_get_parser.parse_args()

        @namespace.expect(controller.flask_restx.query_delete_parser)
        def delete(self):
            return controller.flask_restx.query_delete_parser.parse_args()

    return application


def test_query_get_parser_with_dict(client):
    response = client.get(
        "/test_parsers?dict_col.first_key=2&dict_col.second_key=3&key=4&limit=1&offset=0"
    )
    assert response.json == {
        "dict_col.first_key": ["2"],
        "dict_col.second_key": [3],
        "key": ["4"],
        "limit": 1,
        "offset": 0,
    }


def test_query_delete_parser_with_dict(client):
    response = client.delete(
        "/test_parsers?dict_col.first_key=2&dict_col.second_key=3&key=4"
    )
    assert response.json == {
        "dict_col.first_key": ["2"],
        "dict_col.second_key": [3],
        "key": ["4"],
    }
