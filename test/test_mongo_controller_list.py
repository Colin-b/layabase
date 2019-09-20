import enum
import json

import pytest
from flask_restplus import inputs

from layabase import database, database_mongo
from test.flask_restplus_mock import TestAPI


def parser_types(flask_parser) -> dict:
    return {arg.name: arg.type for arg in flask_parser.args}


def parser_actions(flask_parser) -> dict:
    return {arg.name: arg.action for arg in flask_parser.args}


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestListController(database.CRUDController):
    pass


def _create_models(base):
    class TestListModel(
        database_mongo.CRUDModel, base=base, table_name="list_table_name"
    ):
        key = database_mongo.Column(is_primary_key=True)
        list_field = database_mongo.ListColumn(
            database_mongo.DictColumn(
                fields={
                    "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                    "second_key": database_mongo.Column(int, is_nullable=False),
                }
            )
        )
        bool_field = database_mongo.Column(bool)

    TestListController.model(TestListModel)

    return [TestListModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    TestListController.namespace(TestAPI)

    yield _db

    database.reset(_db)


def test_json_post_model_with_list_of_dict(db):
    assert "TestListModel" == TestListController.json_post_model.name
    assert {
        "bool_field": "Boolean",
        "key": "String",
        "list_field": (
            "List",
            {
                "list_field_inner": (
                    "Nested",
                    {"first_key": "String", "second_key": "Integer"},
                )
            },
        ),
    } == TestListController.json_post_model.fields_flask_type
    assert {
        "bool_field": None,
        "key": None,
        "list_field": (
            None,
            {"list_field_inner": (None, {"first_key": None, "second_key": None})},
        ),
    } == TestListController.json_post_model.fields_description
    assert {
        "bool_field": None,
        "key": None,
        "list_field": (
            None,
            {
                "list_field_inner": (
                    None,
                    {"first_key": ["Value1", "Value2"], "second_key": None},
                )
            },
        ),
    } == TestListController.json_post_model.fields_enum
    assert {
        "bool_field": True,
        "key": "sample key",
        "list_field": (
            [{"first_key": "Value1", "second_key": 1}],
            {
                "list_field_inner": (
                    {"first_key": "Value1", "second_key": 1},
                    {"first_key": "Value1", "second_key": 1},
                )
            },
        ),
    } == TestListController.json_post_model.fields_example
    assert {
        "bool_field": None,
        "key": None,
        "list_field": (
            None,
            {
                "list_field_inner": (
                    {"first_key": None, "second_key": None},
                    {"first_key": None, "second_key": None},
                )
            },
        ),
    } == TestListController.json_post_model.fields_default
    assert {
        "bool_field": False,
        "key": False,
        "list_field": (
            False,
            {"list_field_inner": (False, {"first_key": False, "second_key": False})},
        ),
    } == TestListController.json_post_model.fields_required
    assert {
        "bool_field": False,
        "key": False,
        "list_field": (
            False,
            {"list_field_inner": (False, {"first_key": False, "second_key": False})},
        ),
    } == TestListController.json_post_model.fields_readonly


def test_json_put_model_with_list_of_dict(db):
    assert "TestListModel" == TestListController.json_put_model.name
    assert {
        "bool_field": "Boolean",
        "key": "String",
        "list_field": (
            "List",
            {
                "list_field_inner": (
                    "Nested",
                    {"first_key": "String", "second_key": "Integer"},
                )
            },
        ),
    } == TestListController.json_put_model.fields_flask_type
    assert {
        "bool_field": None,
        "key": None,
        "list_field": (
            None,
            {"list_field_inner": (None, {"first_key": None, "second_key": None})},
        ),
    } == TestListController.json_put_model.fields_description
    assert {
        "bool_field": None,
        "key": None,
        "list_field": (
            None,
            {
                "list_field_inner": (
                    None,
                    {"first_key": ["Value1", "Value2"], "second_key": None},
                )
            },
        ),
    } == TestListController.json_put_model.fields_enum
    assert {
        "bool_field": True,
        "key": "sample key",
        "list_field": (
            [{"first_key": "Value1", "second_key": 1}],
            {
                "list_field_inner": (
                    {"first_key": "Value1", "second_key": 1},
                    {"first_key": "Value1", "second_key": 1},
                )
            },
        ),
    } == TestListController.json_put_model.fields_example
    assert {
        "bool_field": None,
        "key": None,
        "list_field": (
            None,
            {
                "list_field_inner": (
                    {"first_key": None, "second_key": None},
                    {"first_key": None, "second_key": None},
                )
            },
        ),
    } == TestListController.json_put_model.fields_default
    assert {
        "bool_field": False,
        "key": False,
        "list_field": (
            False,
            {"list_field_inner": (False, {"first_key": False, "second_key": False})},
        ),
    } == TestListController.json_put_model.fields_required
    assert {
        "bool_field": False,
        "key": False,
        "list_field": (
            False,
            {"list_field_inner": (False, {"first_key": False, "second_key": False})},
        ),
    } == TestListController.json_put_model.fields_readonly


def test_post_list_of_dict_is_valid(db):
    assert {
        "bool_field": False,
        "key": "my_key",
        "list_field": [
            {"first_key": "Value1", "second_key": 1},
            {"first_key": "Value2", "second_key": 2},
        ],
    } == TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )


def test_post_optional_missing_list_of_dict_is_valid(db):
    assert {
        "bool_field": False,
        "key": "my_key",
        "list_field": None,
    } == TestListController.post({"key": "my_key", "bool_field": False})


def test_post_optional_list_of_dict_as_none_is_valid(db):
    assert {
        "bool_field": False,
        "key": "my_key",
        "list_field": None,
    } == TestListController.post(
        {"key": "my_key", "bool_field": False, "list_field": None}
    )


def test_get_list_of_dict_is_valid(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert [
        {
            "bool_field": False,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value1", "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ],
        }
    ] == TestListController.get(
        {
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ]
        }
    )


def test_get_optional_list_of_dict_as_None_is_skipped(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert [
        {
            "bool_field": False,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value1", "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ],
        }
    ] == TestListController.get({"list_field": None})


def test_delete_list_of_dict_is_valid(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert 1 == TestListController.delete(
        {
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ]
        }
    )


def test_delete_optional_list_of_dict_as_None_is_valid(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert 1 == TestListController.delete({"list_field": None})


def test_put_list_of_dict_is_valid(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert (
        {
            "bool_field": False,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value1", "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ],
        },
        {
            "bool_field": True,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value2", "second_key": 10},
                {"first_key": "Value1", "second_key": 2},
            ],
        },
    ) == TestListController.put(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value2, "second_key": 10},
                {"first_key": EnumTest.Value1, "second_key": 2},
            ],
            "bool_field": True,
        }
    )


def test_put_without_optional_list_of_dict_is_valid(db):
    TestListController.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": EnumTest.Value1, "second_key": 1},
                {"first_key": EnumTest.Value2, "second_key": 2},
            ],
            "bool_field": False,
        }
    )
    assert (
        {
            "bool_field": False,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value1", "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ],
        },
        {
            "bool_field": True,
            "key": "my_key",
            "list_field": [
                {"first_key": "Value1", "second_key": 1},
                {"first_key": "Value2", "second_key": 2},
            ],
        },
    ) == TestListController.put({"key": "my_key", "bool_field": True})


def test_query_get_parser_with_list_of_dict(db):
    assert {
        "bool_field": inputs.boolean,
        "key": str,
        "list_field": json.loads,
        "limit": inputs.positive,
        "offset": inputs.natural,
    } == parser_types(TestListController.query_get_parser)
    assert {
        "bool_field": "append",
        "key": "append",
        "list_field": "append",
        "limit": "store",
        "offset": "store",
    } == parser_actions(TestListController.query_get_parser)


def test_query_delete_parser_with_list_of_dict(db):
    assert {
        "bool_field": inputs.boolean,
        "key": str,
        "list_field": json.loads,
    } == parser_types(TestListController.query_delete_parser)
    assert {
        "bool_field": "append",
        "key": "append",
        "list_field": "append",
    } == parser_actions(TestListController.query_delete_parser)


def test_get_response_model_with_list_of_dict(db):
    assert "TestListModel" == TestListController.get_response_model.name
    assert {
        "bool_field": "Boolean",
        "key": "String",
        "list_field": (
            "List",
            {
                "list_field_inner": (
                    "Nested",
                    {"first_key": "String", "second_key": "Integer"},
                )
            },
        ),
    } == TestListController.get_response_model.fields_flask_type
    assert {
        "bool_field": None,
        "key": None,
        "list_field": (
            None,
            {"list_field_inner": (None, {"first_key": None, "second_key": None})},
        ),
    } == TestListController.get_response_model.fields_description
    assert {
        "bool_field": None,
        "key": None,
        "list_field": (
            None,
            {
                "list_field_inner": (
                    None,
                    {"first_key": ["Value1", "Value2"], "second_key": None},
                )
            },
        ),
    } == TestListController.get_response_model.fields_enum
    assert {
        "bool_field": True,
        "key": "sample key",
        "list_field": (
            [{"first_key": "Value1", "second_key": 1}],
            {
                "list_field_inner": (
                    {"first_key": "Value1", "second_key": 1},
                    {"first_key": "Value1", "second_key": 1},
                )
            },
        ),
    } == TestListController.get_response_model.fields_example
    assert {
        "bool_field": None,
        "key": None,
        "list_field": (
            None,
            {
                "list_field_inner": (
                    {"first_key": None, "second_key": None},
                    {"first_key": None, "second_key": None},
                )
            },
        ),
    } == TestListController.get_response_model.fields_default
    assert {
        "bool_field": False,
        "key": False,
        "list_field": (
            False,
            {"list_field_inner": (False, {"first_key": False, "second_key": False})},
        ),
    } == TestListController.get_response_model.fields_required
    assert {
        "bool_field": False,
        "key": False,
        "list_field": (
            False,
            {"list_field_inner": (False, {"first_key": False, "second_key": False})},
        ),
    } == TestListController.get_response_model.fields_readonly
