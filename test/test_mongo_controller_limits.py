import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo
from test.flask_restplus_mock import TestAPI


class TestLimitsController(database.CRUDController):
    pass


def _create_models(base):
    class TestLimitsModel(
        database_mongo.CRUDModel, base=base, table_name="limits_table_name"
    ):
        key = database_mongo.Column(is_primary_key=True, min_length=3, max_length=4)
        list_field = database_mongo.Column(
            list, min_length=2, max_length=3, example=["my", "test"]
        )
        dict_field = database_mongo.Column(
            dict, min_length=2, max_length=3, example={"my": 1, "test": 2}
        )
        int_field = database_mongo.Column(int, min_value=100, max_value=999)
        float_field = database_mongo.Column(float, min_value=1.25, max_value=1.75)

    TestLimitsController.model(TestLimitsModel)

    return [TestLimitsModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    TestLimitsController.namespace(TestAPI)

    yield _db

    database.reset(_db)


class DateTimeModuleMock:
    class DateTimeMock:
        @staticmethod
        def utcnow():
            class UTCDateTimeMock:
                @staticmethod
                def isoformat():
                    return "2018-10-11T15:05:05.663979"

            return UTCDateTimeMock

    datetime = DateTimeMock


def test_within_limits_is_valid(db):
    assert {
        "dict_field": {"my": 1, "test": 2},
        "int_field": 100,
        "float_field": 1.3,
        "key": "111",
        "list_field": ["1", "2", "3"],
    } == TestLimitsController.post(
        {
            "dict_field": {"my": 1, "test": 2},
            "key": "111",
            "list_field": ["1", "2", "3"],
            "int_field": 100,
            "float_field": 1.3,
        }
    )


def test_outside_upper_limits_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestLimitsController.post(
            {
                "key": "11111",
                "list_field": ["1", "2", "3", "4", "5"],
                "int_field": 1000,
                "float_field": 1.1,
                "dict_field": {"my": 1, "test": 2, "is": 3, "invalid": 4},
            }
        )
    assert {
        "int_field": ['Value "1000" is too big. Maximum value is 999.'],
        "key": ['Value "11111" is too big. Maximum length is 4.'],
        "float_field": ['Value "1.1" is too small. Minimum value is 1.25.'],
        "list_field": [
            "['1', '2', '3', '4', '5'] contains too many values. Maximum length is 3."
        ],
        "dict_field": [
            "{'my': 1, 'test': 2, 'is': 3, 'invalid': 4} contains too many values. Maximum length is 3."
        ],
    } == exception_info.value.errors
    assert {
        "int_field": 1000,
        "float_field": 1.1,
        "key": "11111",
        "list_field": ["1", "2", "3", "4", "5"],
        "dict_field": {"my": 1, "test": 2, "is": 3, "invalid": 4},
    } == exception_info.value.received_data


def test_outside_lower_limits_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestLimitsController.post(
            {
                "key": "11",
                "list_field": ["1"],
                "int_field": 99,
                "dict_field": {"my": 1},
                "float_field": 2.1,
            }
        )
    assert {
        "dict_field": [
            "{'my': 1} does not contains enough values. Minimum length is 2."
        ],
        "int_field": ['Value "99" is too small. Minimum value is 100.'],
        "float_field": ['Value "2.1" is too big. Maximum value is 1.75.'],
        "key": ['Value "11" is too small. Minimum length is 3.'],
        "list_field": ["['1'] does not contains enough values. Minimum length is 2."],
    } == exception_info.value.errors
    assert {
        "key": "11",
        "list_field": ["1"],
        "int_field": 99,
        "dict_field": {"my": 1},
        "float_field": 2.1,
    } == exception_info.value.received_data


def test_get_response_model_with_limits(db):
    assert "TestLimitsModel" == TestLimitsController.get_response_model.name
    assert {
        "dict_field": "Raw",
        "int_field": "Integer",
        "float_field": "Float",
        "key": "String",
        "list_field": ("List", {"list_field_inner": "String"}),
    } == TestLimitsController.get_response_model.fields_flask_type
    assert {
        "dict_field": None,
        "int_field": None,
        "float_field": None,
        "key": None,
        "list_field": (None, {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_description
    assert {
        "dict_field": None,
        "int_field": None,
        "float_field": None,
        "key": None,
        "list_field": (None, {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_enum
    assert {
        "dict_field": {"my": 1, "test": 2},
        "int_field": 100,
        "float_field": 1.4,
        "key": "XXX",
        "list_field": (["my", "test"], {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_example
    assert {
        "dict_field": None,
        "int_field": None,
        "float_field": None,
        "key": None,
        "list_field": (None, {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_default
    assert {
        "dict_field": False,
        "int_field": False,
        "float_field": False,
        "key": False,
        "list_field": (False, {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_required
    assert {
        "dict_field": False,
        "int_field": False,
        "float_field": False,
        "key": False,
        "list_field": (False, {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_readonly
