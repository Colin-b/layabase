import enum

import flask
import flask_restplus
import pytest
from flask_restplus import inputs
from layaberr import ValidationFailed

from layabase import database, database_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestDictController(database.CRUDController):
    class TestDictModel:
        __tablename__ = "dict_table_name"

        key = database_mongo.Column(str, is_primary_key=True)
        dict_col = database_mongo.DictColumn(
            fields={
                "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                "second_key": database_mongo.Column(int, is_nullable=False),
            },
            is_nullable=False,
        )

    model = TestDictModel


@pytest.fixture
def db():
    _db = database.load("mongomock", [TestDictController])
    yield _db
    layabase.testing.reset(_db)


@pytest.fixture
def app(db):
    application = flask.Flask(__name__)
    application.testing = True
    api = flask_restplus.Api(application)
    namespace = api.namespace("Test", path="/")

    TestDictController.namespace(namespace)

    @namespace.route("/test_parsers")
    class TestParsersResource(flask_restplus.Resource):
        @namespace.expect(TestDictController.query_get_parser)
        def get(self):
            return TestDictController.query_get_parser.parse_args()

        @namespace.expect(TestDictController.query_delete_parser)
        def delete(self):
            return TestDictController.query_delete_parser.parse_args()

    return application


def test_post_dict_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )


def test_get_with_dot_notation_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": EnumTest.Value1, "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == TestDictController.get({"dict_col.first_key": EnumTest.Value1})


def test_get_with_dot_notation_as_list_is_valid(db):
    TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": EnumTest.Value1, "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == TestDictController.get({"dict_col.first_key": [EnumTest.Value1]})


def test_get_with_multiple_results_dot_notation_as_list_is_valid(db):
    TestDictController.post_many(
        [
            {
                "key": "my_key",
                "dict_col": {"first_key": EnumTest.Value1, "second_key": 3},
            },
            {
                "key": "my_key2",
                "dict_col": {"first_key": EnumTest.Value2, "second_key": 4},
            },
        ]
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
        {"dict_col": {"first_key": "Value2", "second_key": 4}, "key": "my_key2"},
    ] == TestDictController.get(
        {"dict_col.first_key": [EnumTest.Value1, EnumTest.Value2]}
    )


def test_update_with_dot_notation_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
        {"dict_col": {"first_key": "Value1", "second_key": 4}, "key": "my_key"},
    ) == TestDictController.put({"key": "my_key", "dict_col.second_key": 4})


def test_update_with_dot_notation_invalid_value_is_invalid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(ValidationFailed) as exception_info:
        TestDictController.put(
            {"key": "my_key", "dict_col.second_key": "invalid integer"}
        )
    assert {"dict_col.second_key": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "key": "my_key",
        "dict_col.second_key": "invalid integer",
    } == exception_info.value.received_data


def test_delete_with_dot_notation_invalid_value_is_invalid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(ValidationFailed) as exception_info:
        TestDictController.delete({"dict_col.second_key": "invalid integer"})
    assert {"dict_col.second_key": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "dict_col.second_key": "invalid integer"
    } == exception_info.value.received_data


def test_delete_with_dot_notation_valid_value_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == TestDictController.delete({"dict_col.second_key": 3})


def test_delete_with_dot_notation_enum_value_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == TestDictController.delete({"dict_col.first_key": EnumTest.Value1})


def test_post_with_dot_notation_invalid_value_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDictController.post(
            {
                "key": "my_key",
                "dict_col.first_key": "Value1",
                "dict_col.second_key": "invalid integer",
            }
        )
    assert {"dict_col.second_key": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "key": "my_key",
        "dict_col.first_key": "Value1",
        "dict_col.second_key": "invalid integer",
    } == exception_info.value.received_data


def test_post_with_dot_notation_valid_value_is_valid(db):
    assert {
        "key": "my_key",
        "dict_col": {"first_key": "Value2", "second_key": 1},
    } == TestDictController.post(
        {"key": "my_key", "dict_col.first_key": "Value2", "dict_col.second_key": 1}
    )


def test_get_with_unmatching_dot_notation_is_empty(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert [] == TestDictController.get({"dict_col.first_key": "Value2"})


def test_get_with_unknown_dot_notation_returns_everything(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == TestDictController.get({"dict_col.unknown": "Value1"})


def test_delete_with_dot_notation_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == TestDictController.delete({"dict_col.first_key": "Value1"})
    assert [] == TestDictController.get({})


def test_delete_with_unmatching_dot_notation_is_empty(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 0 == TestDictController.delete({"dict_col.first_key": "Value2"})
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == TestDictController.get({})


def test_delete_with_unknown_dot_notation_deletes_everything(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == TestDictController.delete({"dict_col.unknown": "Value2"})
    assert [] == TestDictController.get({})


def test_put_without_primary_key_is_invalid(db):
    TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(ValidationFailed) as exception_info:
        TestDictController.put({"dict_col": {"first_key": "Value2", "second_key": 4}})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {
        "dict_col": {"first_key": "Value2", "second_key": 4}
    } == exception_info.value.received_data


def test_post_dict_with_dot_notation_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col.first_key": "Value1", "dict_col.second_key": 3}
    )


def test_put_dict_with_dot_notation_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
        {"dict_col": {"first_key": "Value2", "second_key": 3}, "key": "my_key"},
    ) == TestDictController.put(
        {"key": "my_key", "dict_col.first_key": EnumTest.Value2}
    )


def test_post_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDictController.post({"key": "my_key", "dict_col": {"first_key": "Value1"}})
    assert {
        "dict_col.second_key": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {
        "key": "my_key",
        "dict_col": {"first_key": "Value1"},
    } == exception_info.value.received_data


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
