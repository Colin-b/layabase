import pytest
import sqlalchemy
import flask
import flask_restplus
from layaberr import ValidationFailed

from layabase import database, database_sqlalchemy


class TestRequiredController(database.CRUDController):
    pass


def _create_models(base):
    class TestRequiredModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "required_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(
            sqlalchemy.Integer,
            nullable=False,
            info={"marshmallow": {"required_on_query": True}},
        )

    TestRequiredController.model(TestRequiredModel)
    return [TestRequiredModel]


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

    TestRequiredController.namespace(namespace)

    @namespace.route("/test")
    class TestResource(flask_restplus.Resource):
        @namespace.expect(TestRequiredController.query_get_parser)
        @namespace.marshal_with(TestRequiredController.get_response_model)
        def get(self):
            return []

        @namespace.expect(TestRequiredController.json_post_model)
        def post(self):
            return []

        @namespace.expect(TestRequiredController.json_put_model)
        def put(self):
            return []

        @namespace.expect(TestRequiredController.query_delete_parser)
        def delete(self):
            return []

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        def get(self):
            return TestRequiredController.query_get_parser.parse_args()

        def delete(self):
            return TestRequiredController.query_delete_parser.parse_args()

    return application


def test_query_get_parser_without_required_field(client):
    response = client.get("/test_parsers")
    assert response.status_code == 400
    assert response.json == {
        "errors": {"mandatory": "Missing required parameter in the query string"},
        "message": "Input payload validation failed",
    }


def test_get_without_required_field(client):
    with pytest.raises(ValidationFailed) as exception_info:
        TestRequiredController.get({})
    assert exception_info.value.received_data == {}
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }


def test_get_with_required_field(client):
    assert TestRequiredController.get({"mandatory": 1}) == []


def test_get_one_without_required_field(client):
    with pytest.raises(ValidationFailed) as exception_info:
        TestRequiredController.get_one({})
    assert exception_info.value.received_data == {}
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }


def test_get_one_with_required_field(client):
    assert TestRequiredController.get_one({"mandatory": 1}) == {}


def test_delete_without_required_field(client):
    with pytest.raises(ValidationFailed) as exception_info:
        TestRequiredController.delete({})
    assert exception_info.value.received_data == {}
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }


def test_delete_with_required_field(client):
    assert TestRequiredController.delete({"mandatory": 1}) == 0


def test_query_get_parser_with_required_field(client):
    response = client.get("/test_parsers?mandatory=1")
    assert response.json == {
        "key": None,
        "limit": None,
        "mandatory": [1],
        "offset": None,
        "order_by": None,
    }


def test_query_delete_parser_without_required_field(client):
    response = client.delete("/test_parsers")
    assert response.status_code == 400
    assert response.json == {
        "errors": {"mandatory": "Missing required parameter in the query string"},
        "message": "Input payload validation failed",
    }


def test_query_delete_parser_with_required_field(client):
    response = client.delete("/test_parsers?mandatory=1")
    assert response.json == {"key": None, "mandatory": [1]}
