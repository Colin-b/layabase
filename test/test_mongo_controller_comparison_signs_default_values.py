import datetime

import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        int_value = layabase.mongo.Column(
            int, allow_comparison_signs=True, default_value=3
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_get_with_interval_and_default_and_equality(controller):
    controller.post_many(
        [
            {"int_value": -10},
            {"int_value": 0},
            {"int_value": None},  # Consider as default: 3
            {"int_value": 4},
            {"int_value": 5},
            {"int_value": 6},
        ]
    )
    assert controller.get(
        {
            "int_value": [
                (layabase.ComparisonSigns.Lower, 2),
                (layabase.ComparisonSigns.GreaterOrEqual, -5),
                3,  # Default value
                5,  # Non default value (equality)
            ]
        }
    ) == [{"int_value": 0}, {"int_value": 3}, {"int_value": 5}]


def test_get_with_default_and_equality(controller):
    controller.post_many(
        [
            {"int_value": -10},
            {"int_value": 0},
            {"int_value": None},  # Consider as default: 3
            {"int_value": 4},
            {"int_value": 5},
            {"int_value": 6},
        ]
    )
    assert controller.get(
        {"int_value": [3, 5]}  # Default value  # Non default value (equality)
    ) == [{"int_value": 3}, {"int_value": 5}]
