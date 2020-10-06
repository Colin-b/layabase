import flask
import flask_restx
import pytest

import layabase
import layabase.mongo
from layabase.testing import mock_mongo_audit_datetime


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        mandatory = layabase.mongo.Column(int, is_nullable=False)
        optional = layabase.mongo.Column(str)

    controller = layabase.CRUDController(
        TestCollection, audit=True, retrieve_user=lambda: flask.g.current_user_name
    )
    layabase.load("mongomock?ssl=True", [controller], replicaSet="globaldb")
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
            flask.g.current_user_name = "test user"
            return controller.post({"key": "audit_test", "mandatory": 1})

        @namespace.expect(controller.flask_restx.json_put_model)
        def put(self):
            flask.g.current_user_name = ""
            return controller.put({"key": "audit_test", "mandatory": 2})

        @namespace.expect(controller.flask_restx.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test/audit")
    class TestAuditResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_get_audit_parser)
        @namespace.marshal_with(controller.flask_restx.get_audit_response_model)
        def get(self):
            return controller.get_audit({})

    @namespace.route("/test_audit_parser")
    class TestAuditParserResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_get_audit_parser)
        def get(self):
            return controller.flask_restx.query_get_audit_parser.parse_args()

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restx.Resource):
        @namespace.expect(controller.flask_restx.query_get_parser)
        def get(self):
            return controller.flask_restx.query_get_parser.parse_args()

        @namespace.expect(controller.flask_restx.query_delete_parser)
        def delete(self):
            return controller.flask_restx.query_delete_parser.parse_args()

    return application


def test_query_get_parser(client):
    response = client.get("/test_parsers?key=1&mandatory=2&optional=3&limit=1&offset=0")
    assert response.json == {
        "key": ["1"],
        "limit": 1,
        "mandatory": [2],
        "offset": 0,
        "optional": ["3"],
    }


def test_query_get_audit_parser(client):
    response = client.get(
        "/test_audit_parser?key=1&mandatory=2&optional=3&limit=1&offset=0&revision=1&audit_action=Update&audit_user=test"
    )
    assert response.json == {
        "audit_action": ["Update"],
        "audit_date_utc": None,
        "audit_user": ["test"],
        "key": ["1"],
        "limit": 1,
        "mandatory": [2],
        "offset": 0,
        "optional": ["3"],
        "revision": [1],
    }


def test_query_delete_parser(client):
    response = client.delete("/test_parsers?key=1&mandatory=2&optional=3")
    assert response.json == {"key": ["1"], "mandatory": [2], "optional": ["3"]}


def test_audit_user_name(client, mock_mongo_audit_datetime):
    client.post("/test")
    client.put("/test")
    response = client.get("/test/audit")
    assert response.json == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "test user",
            "key": "audit_test",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "audit_test",
            "mandatory": 2,
            "optional": None,
            "revision": 2,
        },
    ]
