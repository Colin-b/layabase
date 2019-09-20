import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo
from test.flask_restplus_mock import TestAPI


class TestUnvalidatedListAndDictController(database.CRUDController):
    pass


def _create_models(base):
    class TestUnvalidatedListAndDictModel(
        database_mongo.CRUDModel, base=base, table_name="list_and_dict_table_name"
    ):
        float_key = database_mongo.Column(float, is_primary_key=True)
        float_with_default = database_mongo.Column(float, default_value=34)
        dict_field = database_mongo.Column(dict, is_required=True)
        list_field = database_mongo.Column(list, is_required=True)

    TestUnvalidatedListAndDictController.model(TestUnvalidatedListAndDictModel)

    return [TestUnvalidatedListAndDictModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    TestUnvalidatedListAndDictController.namespace(TestAPI)

    yield _db

    database.reset(_db)


def test_get_response_model_with_float_and_unvalidated_list_and_dict(db):
    assert (
        "TestUnvalidatedListAndDictModel"
        == TestUnvalidatedListAndDictController.get_response_model.name
    )
    assert {
        "dict_field": "Raw",
        "float_key": "Float",
        "float_with_default": "Float",
        "list_field": ("List", {"list_field_inner": "String"}),
    } == TestUnvalidatedListAndDictController.get_response_model.fields_flask_type
    assert {
        "dict_field": None,
        "float_key": None,
        "float_with_default": None,
        "list_field": (None, {"list_field_inner": None}),
    } == TestUnvalidatedListAndDictController.get_response_model.fields_description
    assert {
        "dict_field": None,
        "float_key": None,
        "float_with_default": None,
        "list_field": (None, {"list_field_inner": None}),
    } == TestUnvalidatedListAndDictController.get_response_model.fields_enum
    assert {
        "dict_field": {
            "1st dict_field key": "1st dict_field sample",
            "2nd dict_field key": "2nd dict_field sample",
        },
        "float_key": 1.4,
        "float_with_default": 34,
        "list_field": (
            ["1st list_field sample", "2nd list_field sample"],
            {"list_field_inner": None},
        ),
    } == TestUnvalidatedListAndDictController.get_response_model.fields_example
    assert {
        "dict_field": None,
        "float_key": None,
        "float_with_default": 34,
        "list_field": (None, {"list_field_inner": None}),
    } == TestUnvalidatedListAndDictController.get_response_model.fields_default
    assert {
        "dict_field": True,
        "float_key": False,
        "float_with_default": False,
        "list_field": (True, {"list_field_inner": None}),
    } == TestUnvalidatedListAndDictController.get_response_model.fields_required
    assert {
        "dict_field": False,
        "float_key": False,
        "float_with_default": False,
        "list_field": (False, {"list_field_inner": None}),
    } == TestUnvalidatedListAndDictController.get_response_model.fields_readonly


def test_post_float_as_int(db):
    assert {
        "dict_field": {"any_key": 5},
        "float_key": 1,
        "float_with_default": 34,
        "list_field": [22, "33", 44.55, True],
    } == TestUnvalidatedListAndDictController.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    )


def test_get_float_as_int(db):
    TestUnvalidatedListAndDictController.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    )
    assert {
        "dict_field": {"any_key": 5},
        "float_key": 1,
        "float_with_default": 34,
        "list_field": [22, "33", 44.55, True],
    } == TestUnvalidatedListAndDictController.get_one({"float_key": 1})


def test_put_float_as_int(db):
    TestUnvalidatedListAndDictController.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    )
    assert (
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "float_with_default": 34,
            "list_field": [22, "33", 44.55, True],
        },
        {
            "dict_field": {"any_key": 6},
            "float_key": 1,
            "float_with_default": 35,
            "list_field": [22, "33", 44.55, True],
        },
    ) == TestUnvalidatedListAndDictController.put(
        {"dict_field.any_key": 6, "float_key": 1, "float_with_default": 35}
    )


def test_get_with_required_field_as_none_is_invalid(db):
    TestUnvalidatedListAndDictController.post(
        {
            "dict_field": {"any_key": 5},
            "float_key": 1,
            "list_field": [22, "33", 44.55, True],
        }
    )
    with pytest.raises(ValidationFailed) as exception_info:
        TestUnvalidatedListAndDictController.get({"dict_field": None})
    assert exception_info.value.errors == {
        "dict_field": ["Missing data for required field."]
    }
    assert {"dict_field": None} == exception_info.value.received_data
