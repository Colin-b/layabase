import enum

import pytest
from flask_restplus import inputs
from layaberr import ValidationFailed

from layabase import database, database_mongo, versioning_mongo
from layabase.database_mongo import _validate_int
from test.flask_restplus_mock import TestAPI


def parser_types(flask_parser) -> dict:
    return {arg.name: arg.type for arg in flask_parser.args}


def parser_actions(flask_parser) -> dict:
    return {arg.name: arg.action for arg in flask_parser.args}


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestVersionedController(database.CRUDController):
    pass


def _create_models(base):
    class TestVersionedModel(
        versioning_mongo.VersionedCRUDModel,
        base=base,
        table_name="versioned_table_name",
    ):
        key = database_mongo.Column(is_primary_key=True)
        dict_field = database_mongo.DictColumn(
            fields={
                "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                "second_key": database_mongo.Column(int, is_nullable=False),
            },
            is_required=True,
        )

    TestVersionedController.model(TestVersionedModel)

    return [TestVersionedModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    TestVersionedController.namespace(TestAPI)

    yield _db

    database.reset(_db)


def test_get_url_with_primary_key_in_model_and_many_models(db):
    models = [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        },
        {
            "key": "second",
            "dict_field": {"first_key": "Value2", "second_key": 2},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        },
    ]
    assert (
        TestVersionedController.get_url("/test", *models)
        == "/test?key=first&key=second"
    )


def test_get_url_with_primary_key_in_model_and_a_single_model(db):
    model = {
        "key": "first",
        "dict_field": {"first_key": "Value1", "second_key": 1},
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }
    assert TestVersionedController.get_url("/test", model) == "/test?key=first"


def test_get_url_with_primary_key_in_model_and_no_model(db):
    assert TestVersionedController.get_url("/test") == "/test"


def test_post_versioning_is_valid(db):
    assert {
        "key": "first",
        "dict_field": {"first_key": "Value1", "second_key": 1},
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    } == TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        }
    ] == TestVersionedController.get_history({})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        }
    ] == TestVersionedController.get({})


def test_post_without_providing_required_nullable_dict_column_is_valid(db):
    assert {
        "dict_field": {"first_key": None, "second_key": None},
        "key": "first",
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    } == TestVersionedController.post({"key": "first"})


def test_put_without_providing_required_nullable_dict_column_is_valid(db):
    TestVersionedController.post(
        {"key": "first", "dict_field": {"first_key": "Value1", "second_key": 0}}
    )
    assert (
        {
            "dict_field": {"first_key": "Value1", "second_key": 0},
            "key": "first",
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 0},
            "key": "first",
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
    ) == TestVersionedController.put({"key": "first"})


def test_put_versioning_is_valid(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert (
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
    ) == TestVersionedController.put(
        {"key": "first", "dict_field.first_key": EnumTest.Value2}
    )
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
    ] == TestVersionedController.get_history({})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        }
    ] == TestVersionedController.get({})


def test_delete_versioning_is_valid(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    TestVersionedController.put(
        {"key": "first", "dict_field.first_key": EnumTest.Value2}
    )
    assert 1 == TestVersionedController.delete({"key": "first"})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": 3,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
    ] == TestVersionedController.get_history({})
    assert [] == TestVersionedController.get({})


def test_rollback_deleted_versioning_is_valid(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    TestVersionedController.put(
        {"key": "first", "dict_field.first_key": EnumTest.Value2}
    )
    before_delete = 2
    TestVersionedController.delete({"key": "first"})
    assert 1 == TestVersionedController.rollback_to({"revision": before_delete})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": 3,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 4,
            "valid_until_revision": -1,
        },
    ] == TestVersionedController.get_history({})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 4,
            "valid_until_revision": -1,
        }
    ] == TestVersionedController.get({})


def test_rollback_before_update_deleted_versioning_is_valid(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    before_update = 1
    TestVersionedController.put(
        {"key": "first", "dict_field.first_key": EnumTest.Value2}
    )
    TestVersionedController.delete({"key": "first"})
    assert 1 == TestVersionedController.rollback_to({"revision": before_update})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": 3,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 4,
            "valid_until_revision": -1,
        },
    ] == TestVersionedController.get_history({})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 4,
            "valid_until_revision": -1,
        }
    ] == TestVersionedController.get({})


def test_rollback_already_valid_versioning_is_valid(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    TestVersionedController.put(
        {"key": "first", "dict_field.first_key": EnumTest.Value2}
    )

    assert 0 == TestVersionedController.rollback_to({"revision": 2})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
    ] == TestVersionedController.get_history({})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        }
    ] == TestVersionedController.get({})


def test_rollback_unknown_criteria_is_valid(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    before_update = 1
    TestVersionedController.put(
        {"key": "first", "dict_field.first_key": EnumTest.Value2}
    )

    assert 0 == TestVersionedController.rollback_to(
        {"revision": before_update, "key": "unknown"}
    )
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
    ] == TestVersionedController.get_history({})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        }
    ] == TestVersionedController.get({})


def test_versioned_many(db):
    TestVersionedController.post_many(
        [
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            },
            {
                "key": "second",
                "dict_field.first_key": EnumTest.Value2,
                "dict_field.second_key": 2,
            },
        ]
    )
    TestVersionedController.put_many(
        [
            {"key": "first", "dict_field.first_key": EnumTest.Value2},
            {"key": "second", "dict_field.second_key": 3},
        ]
    )

    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
        {
            "key": "second",
            "dict_field": {"first_key": "Value2", "second_key": 3},
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
        {
            "key": "second",
            "dict_field": {"first_key": "Value2", "second_key": 2},
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
    ] == TestVersionedController.get_history({})


def test_rollback_without_revision_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestVersionedController.rollback_to({"key": "unknown"})
    assert {
        "revision": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "unknown"} == exception_info.value.received_data


def test_rollback_with_non_int_revision_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestVersionedController.rollback_to({"revision": "invalid revision"})
    assert {"revision": ["Not a valid int."]} == exception_info.value.errors
    assert {"revision": "invalid revision"} == exception_info.value.received_data


def test_rollback_with_negative_revision_is_valid(db):
    assert 0 == TestVersionedController.rollback_to({"revision": -1})


def test_rollback_before_existing_is_valid(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    before_insert = 1
    TestVersionedController.post(
        {
            "key": "second",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert 1 == TestVersionedController.rollback_to({"revision": before_insert})
    assert [] == TestVersionedController.get({"key": "second"})


def test_get_revision_is_valid_when_empty(db):
    assert 0 == TestVersionedController._model.current_revision()


def test_get_revision_is_valid_when_1(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert 1 == TestVersionedController._model.current_revision()


def test_get_revision_is_valid_when_2(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    TestVersionedController.post(
        {
            "key": "second",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert 2 == TestVersionedController._model.current_revision()


def test_rollback_to_0(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    TestVersionedController.post(
        {
            "key": "second",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    assert 2 == TestVersionedController.rollback_to({"revision": 0})
    assert [] == TestVersionedController.get({})


def test_rollback_multiple_rows_is_valid(db):
    TestVersionedController.post(
        {
            "key": "1",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    TestVersionedController.post(
        {
            "key": "2",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    TestVersionedController.put({"key": "1", "dict_field.first_key": EnumTest.Value2})
    TestVersionedController.delete({"key": "2"})
    TestVersionedController.post(
        {
            "key": "3",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    TestVersionedController.post(
        {
            "key": "4",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    before_insert = 6
    TestVersionedController.post(
        {
            "key": "5",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    TestVersionedController.put({"key": "1", "dict_field.second_key": 2})
    # Remove key 5 and Update key 1 (Key 3 and Key 4 unchanged)
    assert 2 == TestVersionedController.rollback_to({"revision": before_insert})
    assert [
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "3",
            "valid_since_revision": 5,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "4",
            "valid_since_revision": 6,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "key": "1",
            "valid_since_revision": 9,
            "valid_until_revision": -1,
        },
    ] == TestVersionedController.get({})
    assert [
        {
            "dict_field": {"first_key": "Value2", "second_key": 2},
            "key": "1",
            "valid_since_revision": 8,
            "valid_until_revision": 9,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "2",
            "valid_since_revision": 2,
            "valid_until_revision": 4,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "1",
            "valid_since_revision": 1,
            "valid_until_revision": 3,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "3",
            "valid_since_revision": 5,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "4",
            "valid_since_revision": 6,
            "valid_until_revision": -1,
        },
        {
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "key": "5",
            "valid_since_revision": 7,
            "valid_until_revision": 9,
        },
        {
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "key": "1",
            "valid_since_revision": 3,
            "valid_until_revision": 8,
        },
        {
            "dict_field": {"first_key": "Value2", "second_key": 1},
            "key": "1",
            "valid_since_revision": 9,
            "valid_until_revision": -1,
        },
    ] == TestVersionedController.get_history({})


def test_json_post_model_versioned(db):
    assert (
        "TestVersionedModel_Versioned" == TestVersionedController.json_post_model.name
    )
    assert {
        "dict_field": ("Nested", {"first_key": "String", "second_key": "Integer"}),
        "key": "String",
    } == TestVersionedController.json_post_model.fields_flask_type
    assert {
        "dict_field": (None, {"first_key": None, "second_key": None}),
        "key": None,
    } == TestVersionedController.json_post_model.fields_description
    assert {
        "dict_field": (None, {"first_key": ["Value1", "Value2"], "second_key": None}),
        "key": None,
    } == TestVersionedController.json_post_model.fields_enum
    assert {
        "dict_field": (
            {"first_key": "Value1", "second_key": 1},
            {"first_key": "Value1", "second_key": 1},
        ),
        "key": "sample key",
    } == TestVersionedController.json_post_model.fields_example
    assert {
        "dict_field": (
            {"first_key": None, "second_key": None},
            {"first_key": None, "second_key": None},
        ),
        "key": None,
    } == TestVersionedController.json_post_model.fields_default
    assert {
        "dict_field": (True, {"first_key": False, "second_key": False}),
        "key": False,
    } == TestVersionedController.json_post_model.fields_required
    assert {
        "dict_field": (False, {"first_key": False, "second_key": False}),
        "key": False,
    } == TestVersionedController.json_post_model.fields_readonly


def test_query_rollback_parser(db):
    assert {
        "dict_field.first_key": str,
        "dict_field.second_key": _validate_int,
        "key": str,
        "revision": inputs.positive,
    } == parser_types(TestVersionedController.query_rollback_parser)
    assert {
        "dict_field.first_key": "append",
        "dict_field.second_key": "append",
        "key": "append",
        "revision": "store",
    } == parser_actions(TestVersionedController.query_rollback_parser)
