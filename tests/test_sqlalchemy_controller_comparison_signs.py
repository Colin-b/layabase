import datetime

import pytest

import layabase
import sqlalchemy


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

        int_value = sqlalchemy.Column(
            sqlalchemy.Integer, info={"layabase": {"allow_comparison_signs": True}}
        )
        float_value = sqlalchemy.Column(
            sqlalchemy.Float, info={"layabase": {"allow_comparison_signs": True}}
        )
        date_value = sqlalchemy.Column(
            sqlalchemy.Date, info={"layabase": {"allow_comparison_signs": True}}
        )
        datetime_value = sqlalchemy.Column(
            sqlalchemy.DateTime, info={"layabase": {"allow_comparison_signs": True}}
        )

    controller = layabase.CRUDController(TestTable)
    layabase.load("sqlite:///:memory:", [controller])
    return controller


def test_get_is_valid_with_int_and_less_than_sign_as_tuple_in_int_column(controller: layabase.CRUDController):
    controller.post_many(
        [
            {"id": "1", "int_value": 122},
            {"id": "2", "int_value": 123},
            {"id": "3", "int_value": 124},
        ]
    )
    assert controller.get({"int_value": (layabase.ComparisonSigns.Lower, 124)}) == [
        {
            "id": "1",
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_float_and_less_than_sign_as_tuple_in_float_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"id": "1", "float_value": 0.9},
            {"id": "2", "float_value": 1.0},
            {"id": "3", "float_value": 1.1},
        ]
    )
    assert controller.get({"float_value": (layabase.ComparisonSigns.Lower, 1.1)}) == [
        {
            "id": "1",
            "int_value": None,
            "float_value": 0.9,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": None,
            "float_value": 1.0,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_date_and_less_than_sign_as_tuple_in_date_column(controller: layabase.CRUDController):
    controller.post_many(
        [
            {"id": "1", "date_value": "2019-01-01"},
            {"id": "2", "date_value": "2019-01-02"},
            {"id": "3", "date_value": "2019-01-03"},
        ]
    )
    assert controller.get(
        {"date_value": (layabase.ComparisonSigns.Lower, datetime.date(2019, 1, 3))}
    ) == [
        {
            "id": "1",
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-01",
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-02",
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_datetime_and_less_than_sign_as_tuple_in_datetime_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"id": "1", "datetime_value": "2019-01-01T23:59:59"},
            {"id": "2", "datetime_value": "2019-01-02T23:59:59"},
            {"id": "3", "datetime_value": "2019-01-03T23:59:59"},
        ]
    )
    assert controller.get(
        {
            "datetime_value": (
                layabase.ComparisonSigns.Lower,
                datetime.datetime(2019, 1, 3, 23, 59, 59),
            )
        }
    ) == [
        {
            "id": "1",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-01T23:59:59",
        },
        {
            "id": "2",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-02T23:59:59",
        },
    ]


def test_get_is_valid_with_int_and_greater_than_sign_as_tuple_in_int_column(controller: layabase.CRUDController):
    controller.post_many(
        [
            {"id": "1", "int_value": 122},
            {"id": "2", "int_value": 123},
            {"id": "3", "int_value": 124},
        ]
    )
    assert controller.get({"int_value": (layabase.ComparisonSigns.Greater, 122)}) == [
        {
            "id": "2",
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "3",
            "int_value": 124,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_float_and_greater_than_sign_as_tuple_in_float_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"id": "1", "float_value": 0.9},
            {"id": "2", "float_value": 1.0},
            {"id": "3", "float_value": 1.1},
        ]
    )
    assert controller.get({"float_value": (layabase.ComparisonSigns.Greater, 0.9)}) == [
        {
            "id": "2",
            "int_value": None,
            "float_value": 1.0,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "3",
            "int_value": None,
            "float_value": 1.1,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_date_and_greater_than_sign_as_tuple_in_date_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"id": "1", "date_value": "2019-01-01"},
            {"id": "2", "date_value": "2019-01-02"},
            {"id": "3", "date_value": "2019-01-03"},
        ]
    )
    assert controller.get(
        {
            "date_value": (
                layabase.ComparisonSigns.Greater,
                datetime.datetime(2019, 1, 1, 0, 0, 0),
            )
        }
    ) == [
        {
            "id": "2",
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-02",
            "datetime_value": None,
        },
        {
            "id": "3",
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-03",
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_datetime_and_greater_than_sign_as_tuple_in_datetime_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"id": "1", "datetime_value": "2019-01-01T23:59:59"},
            {"id": "2", "datetime_value": "2019-01-02T23:59:59"},
            {"id": "3", "datetime_value": "2019-01-03T23:59:59"},
        ]
    )
    assert controller.get(
        {
            "datetime_value": (
                layabase.ComparisonSigns.Greater,
                datetime.datetime(2019, 1, 1, 23, 59, 59),
            )
        }
    ) == [
        {
            "id": "2",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-02T23:59:59",
        },
        {
            "id": "3",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-03T23:59:59",
        },
    ]


def test_get_is_valid_with_int_and_less_than_or_equal_sign_as_tuple_in_int_column(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"id": "1", "int_value": 122},
            {"id": "2", "int_value": 123},
            {"id": "3", "int_value": 124},
        ]
    )
    assert controller.get(
        {"int_value": (layabase.ComparisonSigns.LowerOrEqual, 124)}
    ) == [
        {
            "id": "1",
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "3",
            "int_value": 124,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_float_and_less_than_or_equal_sign_as_tuple_in_float_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "float_value": 0.9},
            {"id": "2", "float_value": 1.0},
            {"id": "3", "float_value": 1.1},
        ]
    )
    assert controller.get(
        {"float_value": (layabase.ComparisonSigns.LowerOrEqual, 1.1)}
    ) == [
        {
            "id": "1",
            "int_value": None,
            "float_value": 0.9,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": None,
            "float_value": 1.0,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "3",
            "int_value": None,
            "float_value": 1.1,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_date_and_less_than_or_equal_sign_as_tuple_in_date_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "date_value": "2019-01-01"},
            {"id": "2", "date_value": "2019-01-02"},
            {"id": "3", "date_value": "2019-01-03"},
        ]
    )
    assert controller.get(
        {
            "date_value": (
                layabase.ComparisonSigns.LowerOrEqual,
                datetime.datetime(2019, 1, 3, 0, 0, 0),
            )
        }
    ) == [
        {
            "id": "1",
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-01",
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-02",
            "datetime_value": None,
        },
        {
            "id": "3",
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-03",
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_datetime_and_less_than_or_equal_sign_as_tuple_in_datetime_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "datetime_value": "2019-01-01T23:59:59"},
            {"id": "2", "datetime_value": "2019-01-02T23:59:59"},
            {"id": "3", "datetime_value": "2019-01-03T23:59:59"},
        ]
    )
    assert controller.get(
        {
            "datetime_value": (
                layabase.ComparisonSigns.LowerOrEqual,
                datetime.datetime(2019, 1, 3, 23, 59, 59),
            )
        }
    ) == [
        {
            "id": "1",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-01T23:59:59",
        },
        {
            "id": "2",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-02T23:59:59",
        },
        {
            "id": "3",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-03T23:59:59",
        },
    ]


def test_get_is_valid_with_int_and_greater_than_or_equal_sign_as_tuple_in_int_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "int_value": 122},
            {"id": "2", "int_value": 123},
            {"id": "3", "int_value": 124},
        ]
    )
    assert controller.get(
        {"int_value": (layabase.ComparisonSigns.GreaterOrEqual, 122)}
    ) == [
        {
            "id": "1",
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "3",
            "int_value": 124,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_float_and_greater_than_or_equal_sign_as_tuple_in_float_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "float_value": 0.9},
            {"id": "2", "float_value": 1.0},
            {"id": "3", "float_value": 1.1},
        ]
    )
    assert [
        {
            "id": "1",
            "int_value": None,
            "float_value": 0.9,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": None,
            "float_value": 1.0,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "3",
            "int_value": None,
            "float_value": 1.1,
            "date_value": None,
            "datetime_value": None,
        },
    ] == controller.get({"float_value": (layabase.ComparisonSigns.GreaterOrEqual, 0.9)})


def test_get_is_valid_with_date_and_greater_than_or_equal_sign_as_tuple_in_date_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "date_value": "2019-01-01"},
            {"id": "2", "date_value": "2019-01-02"},
            {"id": "3", "date_value": "2019-01-03"},
        ]
    )
    assert [
        {
            "id": "1",
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-01",
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-02",
            "datetime_value": None,
        },
        {
            "id": "3",
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-03",
            "datetime_value": None,
        },
    ] == controller.get(
        {
            "date_value": (
                layabase.ComparisonSigns.GreaterOrEqual,
                datetime.date(2019, 1, 1),
            )
        }
    )


def test_get_is_valid_with_datetime_and_greater_than_or_equal_sign_as_tuple_in_datetime_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "datetime_value": "2019-01-01T23:59:59"},
            {"id": "2", "datetime_value": "2019-01-02T23:59:59"},
            {"id": "3", "datetime_value": "2019-01-03T23:59:59"},
        ]
    )
    assert [
        {
            "id": "1",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-01T23:59:59",
        },
        {
            "id": "2",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-02T23:59:59",
        },
        {
            "id": "3",
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


def test_get_is_invalid_with_int_and_unknown_as_tuple_in_int_column(controller):
    with pytest.raises(KeyError) as exception_info:
        controller.get({"int_value": ("test", 124)})
    assert str(exception_info.value) == "'test'"


def test_get_is_invalid_with_float_and_unknown_as_tuple_in_float_column(controller):
    with pytest.raises(KeyError) as exception_info:
        controller.get({"float_value": ("test", 1.1)})
    assert str(exception_info.value) == "'test'"


def test_get_is_invalid_with_date_and_unknown_as_tuple_in_date_column(controller):
    with pytest.raises(KeyError) as exception_info:
        controller.get({"date_value": ("test", datetime.date(2019, 1, 3))})
    assert str(exception_info.value) == "'test'"


def test_get_is_invalid_with_datetime_and_unknown_as_tuple_in_datetime_column(
    controller,
):
    with pytest.raises(KeyError) as exception_info:
        controller.get(
            {"datetime_value": ("test", datetime.datetime(2019, 1, 3, 23, 59, 59))}
        )
    assert str(exception_info.value) == "'test'"


def test_get_is_valid_with_int_range_using_comparison_signs_as_tuple_in_int_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "int_value": 122},
            {"id": "2", "int_value": 123},
            {"id": "3", "int_value": 124},
        ]
    )
    assert controller.get(
        {
            "int_value": [
                (layabase.ComparisonSigns.GreaterOrEqual, 122),
                (layabase.ComparisonSigns.Lower, 124),
            ]
        }
    ) == [
        {
            "id": "1",
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_float_range_using_comparison_signs_as_tuple_in_float_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "float_value": 0.9},
            {"id": "2", "float_value": 1.0},
            {"id": "3", "float_value": 1.1},
        ]
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
            "id": "2",
            "int_value": None,
            "float_value": 1.0,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "3",
            "int_value": None,
            "float_value": 1.1,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_date_range_using_comparison_signs_as_tuple_in_date_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "date_value": "2019-01-01"},
            {"id": "2", "date_value": "2019-01-02"},
            {"id": "3", "date_value": "2019-01-03"},
        ]
    )
    assert controller.get(
        {
            "date_value": [
                (layabase.ComparisonSigns.Greater, datetime.date(2019, 1, 1)),
                (layabase.ComparisonSigns.Lower, datetime.date(2019, 1, 3)),
            ]
        }
    ) == [
        {
            "id": "2",
            "int_value": None,
            "float_value": None,
            "date_value": "2019-01-02",
            "datetime_value": None,
        }
    ]


def test_get_is_valid_with_datetime_range_using_comparison_signs_as_tuple_in_datetime_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "datetime_value": "2019-01-01T23:59:59"},
            {"id": "2", "datetime_value": "2019-01-02T23:59:59"},
            {"id": "3", "datetime_value": "2019-01-03T23:59:59"},
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
            "id": "1",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-01T23:59:59",
        },
        {
            "id": "2",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-02T23:59:59",
        },
        {
            "id": "3",
            "int_value": None,
            "float_value": None,
            "date_value": None,
            "datetime_value": "2019-01-03T23:59:59",
        },
    ]


def test_get_is_valid_with_int_range_and_value_out_of_range_using_comparison_signs_as_tuple_in_int_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "int_value": 122},
            {"id": "2", "int_value": 123},
            {"id": "3", "int_value": 124},
            {"id": "4", "int_value": 125},
        ]
    )
    assert controller.get(
        {
            "int_value": [
                (layabase.ComparisonSigns.GreaterOrEqual, 122),
                (layabase.ComparisonSigns.Lower, 124),
                125,
            ]
        }
    ) == [
        {
            "id": "1",
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "4",
            "int_value": 125,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ]


def test_get_is_valid_with_int_range_and_multiple_values_out_of_range_using_comparison_signs_as_tuple_in_int_column(
    controller,
):
    controller.post_many(
        [
            {"id": "1", "int_value": 122},
            {"id": "2", "int_value": 123},
            {"id": "3", "int_value": 124},
            {"id": "4", "int_value": 125},
            {"id": "5", "int_value": 126},
        ]
    )
    assert controller.get(
        {
            "int_value": [
                (layabase.ComparisonSigns.GreaterOrEqual, 122),
                (layabase.ComparisonSigns.Lower, 124),
                125,
                126,
            ]
        }
    ) == [
        {
            "id": "1",
            "int_value": 122,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "2",
            "int_value": 123,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "4",
            "int_value": 125,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
        {
            "id": "5",
            "int_value": 126,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ]
