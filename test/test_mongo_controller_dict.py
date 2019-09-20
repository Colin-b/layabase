import enum

import pytest
from flask_restplus import inputs
from layaberr import ValidationFailed

from layabase import database, database_mongo
from layabase.database_mongo import _validate_int
from test.flask_restplus_mock import TestAPI


def parser_types(flask_parser) -> dict:
    return {arg.name: arg.type for arg in flask_parser.args}


def parser_actions(flask_parser) -> dict:
    return {arg.name: arg.action for arg in flask_parser.args}


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestDictController(database.CRUDController):
    pass


def _create_models(base):
    class TestDictModel(
        database_mongo.CRUDModel, base=base, table_name="dict_table_name"
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        dict_col = database_mongo.DictColumn(
            fields={
                "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                "second_key": database_mongo.Column(int, is_nullable=False),
            },
            is_nullable=False,
        )

    TestDictController.model(TestDictModel)

    return [TestDictModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    TestDictController.namespace(TestAPI)

    yield _db

    database.reset(_db)


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


def test_query_get_parser_with_dict(db):
    assert {
        "dict_col.first_key": str,
        "dict_col.second_key": _validate_int,
        "key": str,
        "limit": inputs.positive,
        "offset": inputs.natural,
    } == parser_types(TestDictController.query_get_parser)
    assert {
        "dict_col.first_key": "append",
        "dict_col.second_key": "append",
        "key": "append",
        "limit": "store",
        "offset": "store",
    } == parser_actions(TestDictController.query_get_parser)


def test_query_delete_parser_with_dict(db):
    assert {
        "dict_col.first_key": str,
        "dict_col.second_key": _validate_int,
        "key": str,
    } == parser_types(TestDictController.query_delete_parser)
    assert {
        "dict_col.first_key": "append",
        "dict_col.second_key": "append",
        "key": "append",
    } == parser_actions(TestDictController.query_delete_parser)
