import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(is_primary_key=True, min_length=3, max_length=4)
        list_field = layabase.mongo.Column(
            list, min_length=2, max_length=3, example=["my", "test"]
        )
        dict_field = layabase.mongo.Column(
            dict, min_length=2, max_length=3, example={"my": 1, "test": 2}
        )
        int_field = layabase.mongo.Column(int, min_value=100, max_value=999)
        float_field = layabase.mongo.Column(float, min_value=1.25, max_value=1.75)

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_within_limits_is_valid(controller):
    assert controller.post(
        {
            "dict_field": {"my": 1, "test": 2},
            "key": "111",
            "list_field": ["1", "2", "3"],
            "int_field": 100,
            "float_field": 1.3,
        }
    ) == {
        "dict_field": {"my": 1, "test": 2},
        "int_field": 100,
        "float_field": 1.3,
        "key": "111",
        "list_field": ["1", "2", "3"],
    }


def test_outside_upper_limits_is_invalid(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post(
            {
                "key": "11111",
                "list_field": ["1", "2", "3", "4", "5"],
                "int_field": 1000,
                "float_field": 1.1,
                "dict_field": {"my": 1, "test": 2, "is": 3, "invalid": 4},
            }
        )
    assert exception_info.value.errors == {
        "int_field": ['Value "1000" is too big. Maximum value is 999.'],
        "key": ['Value "11111" is too big. Maximum length is 4.'],
        "float_field": ['Value "1.1" is too small. Minimum value is 1.25.'],
        "list_field": [
            "['1', '2', '3', '4', '5'] contains too many values. Maximum length is 3."
        ],
        "dict_field": [
            "{'my': 1, 'test': 2, 'is': 3, 'invalid': 4} contains too many values. Maximum length is 3."
        ],
    }
    assert exception_info.value.received_data == {
        "int_field": 1000,
        "float_field": 1.1,
        "key": "11111",
        "list_field": ["1", "2", "3", "4", "5"],
        "dict_field": {"my": 1, "test": 2, "is": 3, "invalid": 4},
    }


def test_outside_lower_limits_is_invalid(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post(
            {
                "key": "11",
                "list_field": ["1"],
                "int_field": 99,
                "dict_field": {"my": 1},
                "float_field": 2.1,
            }
        )
    assert exception_info.value.errors == {
        "dict_field": [
            "{'my': 1} does not contains enough values. Minimum length is 2."
        ],
        "int_field": ['Value "99" is too small. Minimum value is 100.'],
        "float_field": ['Value "2.1" is too big. Maximum value is 1.75.'],
        "key": ['Value "11" is too small. Minimum length is 3.'],
        "list_field": ["['1'] does not contains enough values. Minimum length is 2."],
    }
    assert exception_info.value.received_data == {
        "key": "11",
        "list_field": ["1"],
        "int_field": 99,
        "dict_field": {"my": 1},
        "float_field": 2.1,
    }
