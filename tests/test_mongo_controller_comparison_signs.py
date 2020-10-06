import datetime

import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        int_value = layabase.mongo.Column(int, allow_comparison_signs=True)
        float_value = layabase.mongo.Column(float, allow_comparison_signs=True)
        date_value = layabase.mongo.Column(datetime.date, allow_comparison_signs=True)
        datetime_value = layabase.mongo.Column(
            datetime.datetime, allow_comparison_signs=True
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_get_is_valid_with_int_and_less_than_sign_as_tuple_in_int_column(controller: layabase.CRUDController):
    controller.post_many([{"int_value": 122}, {"int_value": 123}, {"int_value": 124}])
    assert [
        {
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ] == controller.get({"int_value": (layabase.ComparisonSigns.Lower, 124)})


def test_get_is_valid_with_float_and_less_than_sign_as_tuple_in_float_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [{"float_value": 0.9}, {"float_value": 1.0}, {"float_value": 1.1}]
    )
    assert [
        {
            "int_value": None,
            "float_value": 0.9,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": 1.0,
            "date_value": None,
            "datetime_value": None,
        },
    ] == controller.get({"float_value": (layabase.ComparisonSigns.Lower, 1.1)})


def test_get_is_valid_with_date_and_less_than_sign_as_tuple_in_date_column(controller: layabase.CRUDController):
    controller.post_many(
        [
            {"date_value": "2019-01-01"},
            {"date_value": "2019-01-02"},
            {"date_value": "2019-01-03"},
        ]
    )
    assert [
        {
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-01",
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-02",
            "datetime_value": None,
        },
    ] == controller.get(
        {
            "date_value": (
                layabase.ComparisonSigns.Lower,
                datetime.datetime(2019, 1, 3, 0, 0, 0),
            )
        }
    )


def test_get_is_valid_with_datetime_and_less_than_sign_as_tuple_in_datetime_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"datetime_value": "2019-01-01T23:59:59"},
            {"datetime_value": "2019-01-02T23:59:59"},
            {"datetime_value": "2019-01-03T23:59:59"},
        ]
    )
    assert [
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-01T23:59:59",
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-02T23:59:59",
        },
    ] == controller.get(
        {
            "datetime_value": (
                layabase.ComparisonSigns.Lower,
                datetime.datetime(2019, 1, 3, 23, 59, 59),
            )
        }
    )


def test_get_is_valid_with_int_and_greater_than_sign_as_tuple_in_int_column(controller: layabase.CRUDController):
    controller.post_many([{"int_value": 122}, {"int_value": 123}, {"int_value": 124}])
    assert [
        {
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 124,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ] == controller.get({"int_value": (layabase.ComparisonSigns.Greater, 122)})


def test_get_is_valid_with_float_and_greater_than_sign_as_tuple_in_float_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [{"float_value": 0.9}, {"float_value": 1.0}, {"float_value": 1.1}]
    )
    assert [
        {
            "int_value": None,
            "float_value": 1.0,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": 1.1,
            "date_value": None,
            "datetime_value": None,
        },
    ] == controller.get({"float_value": (layabase.ComparisonSigns.Greater, 0.9)})


def test_get_is_valid_with_date_and_greater_than_sign_as_tuple_in_date_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"date_value": "2019-01-01"},
            {"date_value": "2019-01-02"},
            {"date_value": "2019-01-03"},
        ]
    )
    assert [
        {
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-02",
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-03",
            "datetime_value": None,
        },
    ] == controller.get(
        {
            "date_value": (
                layabase.ComparisonSigns.Greater,
                datetime.datetime(2019, 1, 1, 0, 0, 0),
            )
        }
    )


def test_get_is_valid_with_datetime_and_greater_than_sign_as_tuple_in_datetime_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"datetime_value": "2019-01-01T23:59:59"},
            {"datetime_value": "2019-01-02T23:59:59"},
            {"datetime_value": "2019-01-03T23:59:59"},
        ]
    )
    assert [
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-02T23:59:59",
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-03T23:59:59",
        },
    ] == controller.get(
        {
            "datetime_value": (
                layabase.ComparisonSigns.Greater,
                datetime.datetime(2019, 1, 1, 23, 59, 59),
            )
        }
    )


def test_get_is_valid_with_int_and_less_than_or_equal_sign_as_tuple_in_int_column(
    controller: layabase.CRUDController,
):
    controller.post_many([{"int_value": 122}, {"int_value": 123}, {"int_value": 124}])
    assert [
        {
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 124,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ] == controller.get({"int_value": (layabase.ComparisonSigns.LowerOrEqual, 124)})


def test_get_is_valid_with_float_and_less_than_or_equal_sign_as_tuple_in_float_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [{"float_value": 0.9}, {"float_value": 1.0}, {"float_value": 1.1}]
    )
    assert [
        {
            "int_value": None,
            "float_value": 0.9,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": 1.0,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": 1.1,
            "date_value": None,
            "datetime_value": None,
        },
    ] == controller.get({"float_value": (layabase.ComparisonSigns.LowerOrEqual, 1.1)})


def test_get_is_valid_with_date_and_less_than_or_equal_sign_as_tuple_in_date_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"date_value": "2019-01-01"},
            {"date_value": "2019-01-02"},
            {"date_value": "2019-01-03"},
        ]
    )
    assert [
        {
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-01",
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-02",
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-03",
            "datetime_value": None,
        },
    ] == controller.get(
        {
            "date_value": (
                layabase.ComparisonSigns.LowerOrEqual,
                datetime.datetime(2019, 1, 3, 0, 0, 0),
            )
        }
    )


def test_get_is_valid_with_datetime_and_less_than_or_equal_sign_as_tuple_in_datetime_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"datetime_value": "2019-01-01T23:59:59"},
            {"datetime_value": "2019-01-02T23:59:59"},
            {"datetime_value": "2019-01-03T23:59:59"},
        ]
    )
    assert [
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-01T23:59:59",
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-02T23:59:59",
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-03T23:59:59",
        },
    ] == controller.get(
        {
            "datetime_value": (
                layabase.ComparisonSigns.LowerOrEqual,
                datetime.datetime(2019, 1, 3, 23, 59, 59),
            )
        }
    )


def test_get_is_valid_with_int_and_greater_than_or_equal_sign_as_tuple_in_int_column(
    controller: layabase.CRUDController,
):
    controller.post_many([{"int_value": 122}, {"int_value": 123}, {"int_value": 124}])
    assert [
        {
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 124,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ] == controller.get({"int_value": (layabase.ComparisonSigns.GreaterOrEqual, 122)})


def test_get_is_valid_with_float_and_greater_than_or_equal_sign_as_tuple_in_float_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [{"float_value": 0.9}, {"float_value": 1.0}, {"float_value": 1.1}]
    )
    assert [
        {
            "int_value": None,
            "float_value": 0.9,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": 1.0,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": 1.1,
            "date_value": None,
            "datetime_value": None,
        },
    ] == controller.get({"float_value": (layabase.ComparisonSigns.GreaterOrEqual, 0.9)})


def test_get_is_valid_with_date_and_greater_than_or_equal_sign_as_tuple_in_date_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"date_value": "2019-01-01"},
            {"date_value": "2019-01-02"},
            {"date_value": "2019-01-03"},
        ]
    )
    assert [
        {
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-01",
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-02",
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-03",
            "datetime_value": None,
        },
    ] == controller.get(
        {
            "date_value": (
                layabase.ComparisonSigns.GreaterOrEqual,
                datetime.datetime(2019, 1, 1, 0, 0, 0),
            )
        }
    )


def test_get_is_valid_with_datetime_and_greater_than_or_equal_sign_as_tuple_in_datetime_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"datetime_value": "2019-01-01T23:59:59"},
            {"datetime_value": "2019-01-02T23:59:59"},
            {"datetime_value": "2019-01-03T23:59:59"},
        ]
    )
    assert [
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-01T23:59:59",
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-02T23:59:59",
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-03T23:59:59",
        },
    ] == controller.get(
        {
            "datetime_value": (
                layabase.ComparisonSigns.GreaterOrEqual,
                datetime.datetime(2019, 1, 1, 23, 59, 59),
            )
        }
    )


def test_get_is_invalid_with_int_and_unknown_as_tuple_in_int_column(controller: layabase.CRUDController):
    with pytest.raises(KeyError) as exception_info:
        controller.get({"int_value": ("test", 124)})
    assert str(exception_info.value) == "'test'"


def test_get_is_invalid_with_float_and_unknown_as_tuple_in_float_column(controller: layabase.CRUDController):
    with pytest.raises(KeyError) as exception_info:
        controller.get({"float_value": ("test", 1.1)})
    assert str(exception_info.value) == "'test'"


def test_get_is_invalid_with_date_and_unknown_as_tuple_in_date_column(controller: layabase.CRUDController):
    with pytest.raises(KeyError) as exception_info:
        controller.get({"date_value": ("test", datetime.date(2019, 1, 3))})
    assert str(exception_info.value) == "'test'"


def test_get_is_invalid_with_datetime_and_unknown_as_tuple_in_datetime_column(
    controller: layabase.CRUDController,
):
    with pytest.raises(KeyError) as exception_info:
        controller.get(
            {"datetime_value": ("test", datetime.datetime(2019, 1, 3, 23, 59, 59))}
        )
    assert str(exception_info.value) == "'test'"


def test_get_is_valid_with_int_range_using_comparison_signs_as_tuple_in_int_column(
    controller: layabase.CRUDController,
):
    controller.post_many([{"int_value": 122}, {"int_value": 123}, {"int_value": 124}])
    assert controller.get(
        {
            "int_value": [
                (layabase.ComparisonSigns.GreaterOrEqual, 122),
                (layabase.ComparisonSigns.Lower, 124),
            ]
        }
    ) == [
        {
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_float_range_using_comparison_signs_as_tuple_in_float_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [{"float_value": 0.9}, {"float_value": 1.0}, {"float_value": 1.1}]
    )
    assert controller.get(
        {
            "float_value": [
                (layabase.ComparisonSigns.Greater, 0.9),
                (layabase.ComparisonSigns.LowerOrEqual, 1.1),
            ]
        }
    ) == [
        {
            "int_value": None,
            "float_value": 1.0,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": None,
            "float_value": 1.1,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_date_range_using_comparison_signs_as_tuple_in_date_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"date_value": "2019-01-01"},
            {"date_value": "2019-01-02"},
            {"date_value": "2019-01-03"},
        ]
    )
    assert controller.get(
        {
            "date_value": [
                (
                    layabase.ComparisonSigns.Greater,
                    datetime.datetime(2019, 1, 1, 0, 0, 0),
                ),
                (
                    layabase.ComparisonSigns.Lower,
                    datetime.datetime(2019, 1, 3, 0, 0, 0),
                ),
            ]
        }
    ) == [
        {
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-02",
            "datetime_value": None,
        }
    ]


def test_get_is_valid_with_datetime_range_using_comparison_signs_as_tuple_in_datetime_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"datetime_value": "2019-01-01T23:59:59"},
            {"datetime_value": "2019-01-02T23:59:59"},
            {"datetime_value": "2019-01-03T23:59:59"},
        ]
    )
    assert controller.get(
        {
            "datetime_value": [
                (
                    layabase.ComparisonSigns.GreaterOrEqual,
                    datetime.datetime(2019, 1, 1, 23, 59, 59),
                ),
                (
                    layabase.ComparisonSigns.LowerOrEqual,
                    datetime.datetime(2019, 1, 3, 23, 59, 59),
                ),
            ]
        }
    ) == [
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-01T23:59:59",
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-02T23:59:59",
        },
        {
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-03T23:59:59",
        },
    ]


def test_get_is_valid_with_int_range_and_value_out_of_range_using_comparison_signs_as_tuple_in_int_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [{"int_value": 122}, {"int_value": 123}, {"int_value": 124}, {"int_value": 125}]
    )
    assert [
        {
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 125,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ] == controller.get(
        {
            "int_value": [
                (layabase.ComparisonSigns.GreaterOrEqual, 122),
                (layabase.ComparisonSigns.Lower, 124),
                125,
            ]
        }
    )


def test_get_is_valid_with_int_range_and_multiple_values_out_of_range_using_comparison_signs_as_tuple_in_int_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"int_value": 122},
            {"int_value": 123},
            {"int_value": 124},
            {"int_value": 125},
            {"int_value": 126},
        ]
    )
    assert [
        {
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 125,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "int_value": 126,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ] == controller.get(
        {
            "int_value": [
                (layabase.ComparisonSigns.GreaterOrEqual, 122),
                (layabase.ComparisonSigns.Lower, 124),
                125,
                126,
            ]
        }
    )
