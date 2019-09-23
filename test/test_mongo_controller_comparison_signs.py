import datetime

import pytest

from layabase import database, database_mongo, ComparisonSigns


class TestSupportForComparisonSignsController(database.CRUDController):
    pass


def _create_models(base):
    class TestSupportForComparisonSignsModel(
        database_mongo.CRUDModel, base=base, table_name="support_comparison_sign"
    ):
        int_value = database_mongo.Column(int)
        float_value = database_mongo.Column(float)
        date_value = database_mongo.Column(datetime.date)
        datetime_value = database_mongo.Column(datetime.datetime)

    TestSupportForComparisonSignsController.model(TestSupportForComparisonSignsModel)

    return [TestSupportForComparisonSignsModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    yield _db
    database.reset(_db)


@pytest.fixture
def app(db):
    import flask
    import flask_restplus

    application = flask.Flask(__name__)
    api = flask_restplus.Api(application)

    namespace = api.namespace("TestNs")
    TestSupportForComparisonSignsController.namespace(namespace)

    @namespace.route("/test")
    class APITest(flask_restplus.Resource):
        @namespace.expect(TestSupportForComparisonSignsController.query_get_parser)
        def get(self):
            pass

    return application


def test_get_is_valid_with_int_and_less_than_sign_as_tuple_in_int_column(db):
    TestSupportForComparisonSignsController.post_many(
        [{"int_value": 122}, {"int_value": 123}, {"int_value": 124}]
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
    ] == TestSupportForComparisonSignsController.get(
        {"int_value": (ComparisonSigns.Lower, 124)}
    )


def test_get_is_valid_with_float_and_less_than_sign_as_tuple_in_float_column(db):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {"float_value": (ComparisonSigns.Lower, 1.1)}
    )


def test_get_is_valid_with_date_and_less_than_sign_as_tuple_in_date_column(db):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {"date_value": (ComparisonSigns.Lower, datetime.datetime(2019, 1, 3, 0, 0, 0))}
    )


def test_get_is_valid_with_datetime_and_less_than_sign_as_tuple_in_datetime_column(db):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "datetime_value": (
                ComparisonSigns.Lower,
                datetime.datetime(2019, 1, 3, 23, 59, 59),
            )
        }
    )


def test_get_is_valid_with_int_and_greater_than_sign_as_tuple_in_int_column(db):
    TestSupportForComparisonSignsController.post_many(
        [{"int_value": 122}, {"int_value": 123}, {"int_value": 124}]
    )
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
    ] == TestSupportForComparisonSignsController.get(
        {"int_value": (ComparisonSigns.Greater, 122)}
    )


def test_get_is_valid_with_float_and_greater_than_sign_as_tuple_in_float_column(db):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {"float_value": (ComparisonSigns.Greater, 0.9)}
    )


def test_get_is_valid_with_date_and_greater_than_sign_as_tuple_in_date_column(db):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "date_value": (
                ComparisonSigns.Greater,
                datetime.datetime(2019, 1, 1, 0, 0, 0),
            )
        }
    )


def test_get_is_valid_with_datetime_and_greater_than_sign_as_tuple_in_datetime_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "datetime_value": (
                ComparisonSigns.Greater,
                datetime.datetime(2019, 1, 1, 23, 59, 59),
            )
        }
    )


def test_get_is_valid_with_int_and_less_than_or_equal_sign_as_tuple_in_int_column(db):
    TestSupportForComparisonSignsController.post_many(
        [{"int_value": 122}, {"int_value": 123}, {"int_value": 124}]
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
            "int_value": 124,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ] == TestSupportForComparisonSignsController.get(
        {"int_value": (ComparisonSigns.LowerOrEqual, 124)}
    )


def test_get_is_valid_with_float_and_less_than_or_equal_sign_as_tuple_in_float_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {"float_value": (ComparisonSigns.LowerOrEqual, 1.1)}
    )


def test_get_is_valid_with_date_and_less_than_or_equal_sign_as_tuple_in_date_column(db):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "date_value": (
                ComparisonSigns.LowerOrEqual,
                datetime.datetime(2019, 1, 3, 0, 0, 0),
            )
        }
    )


def test_get_is_valid_with_datetime_and_less_than_or_equal_sign_as_tuple_in_datetime_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "datetime_value": (
                ComparisonSigns.LowerOrEqual,
                datetime.datetime(2019, 1, 3, 23, 59, 59),
            )
        }
    )


def test_get_is_valid_with_int_and_greater_than_or_equal_sign_as_tuple_in_int_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
        [{"int_value": 122}, {"int_value": 123}, {"int_value": 124}]
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
            "int_value": 124,
            "float_value": None,
            "date_value": None,
            "datetime_value": None,
        },
    ] == TestSupportForComparisonSignsController.get(
        {"int_value": (ComparisonSigns.GreaterOrEqual, 122)}
    )


def test_get_is_valid_with_float_and_greater_than_or_equal_sign_as_tuple_in_float_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {"float_value": (ComparisonSigns.GreaterOrEqual, 0.9)}
    )


def test_get_is_valid_with_date_and_greater_than_or_equal_sign_as_tuple_in_date_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "date_value": (
                ComparisonSigns.GreaterOrEqual,
                datetime.datetime(2019, 1, 1, 0, 0, 0),
            )
        }
    )


def test_get_is_valid_with_datetime_and_greater_than_or_equal_sign_as_tuple_in_datetime_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "datetime_value": (
                ComparisonSigns.GreaterOrEqual,
                datetime.datetime(2019, 1, 1, 23, 59, 59),
            )
        }
    )


def test_get_is_invalid_with_int_and_unknown_as_tuple_in_int_column(db):
    with pytest.raises(KeyError) as exception_info:
        TestSupportForComparisonSignsController.get({"int_value": ("test", 124)})
    assert str(exception_info.value) == "'test'"


def test_get_is_invalid_with_float_and_unknown_as_tuple_in_float_column(db):
    with pytest.raises(KeyError) as exception_info:
        TestSupportForComparisonSignsController.get({"float_value": ("test", 1.1)})
    assert str(exception_info.value) == "'test'"


def test_get_is_invalid_with_date_and_unknown_as_tuple_in_date_column(db):
    with pytest.raises(KeyError) as exception_info:
        TestSupportForComparisonSignsController.get(
            {"date_value": ("test", datetime.date(2019, 1, 3))}
        )
    assert str(exception_info.value) == "'test'"


def test_get_is_invalid_with_datetime_and_unknown_as_tuple_in_datetime_column(db):
    with pytest.raises(KeyError) as exception_info:
        TestSupportForComparisonSignsController.get(
            {"datetime_value": ("test", datetime.datetime(2019, 1, 3, 23, 59, 59))}
        )
    assert str(exception_info.value) == "'test'"


def test_get_is_valid_with_int_range_using_comparison_signs_as_tuple_in_int_column(db):
    TestSupportForComparisonSignsController.post_many(
        [{"int_value": 122}, {"int_value": 123}, {"int_value": 124}]
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "int_value": [
                (ComparisonSigns.GreaterOrEqual, 122),
                (ComparisonSigns.Lower, 124),
            ]
        }
    )


def test_get_is_valid_with_float_range_using_comparison_signs_as_tuple_in_float_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "float_value": [
                (ComparisonSigns.Greater, 0.9),
                (ComparisonSigns.LowerOrEqual, 1.1),
            ]
        }
    )


def test_get_is_valid_with_date_range_using_comparison_signs_as_tuple_in_date_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
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
        }
    ] == TestSupportForComparisonSignsController.get(
        {
            "date_value": [
                (ComparisonSigns.Greater, datetime.datetime(2019, 1, 1, 0, 0, 0)),
                (ComparisonSigns.Lower, datetime.datetime(2019, 1, 3, 0, 0, 0)),
            ]
        }
    )


def test_get_is_valid_with_datetime_range_using_comparison_signs_as_tuple_in_datetime_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "datetime_value": [
                (
                    ComparisonSigns.GreaterOrEqual,
                    datetime.datetime(2019, 1, 1, 23, 59, 59),
                ),
                (
                    ComparisonSigns.LowerOrEqual,
                    datetime.datetime(2019, 1, 3, 23, 59, 59),
                ),
            ]
        }
    )


def test_get_is_valid_with_int_range_and_value_out_of_range_using_comparison_signs_as_tuple_in_int_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "int_value": [
                (ComparisonSigns.GreaterOrEqual, 122),
                (ComparisonSigns.Lower, 124),
                125,
            ]
        }
    )


def test_get_is_valid_with_int_range_and_multiple_values_out_of_range_using_comparison_signs_as_tuple_in_int_column(
    db,
):
    TestSupportForComparisonSignsController.post_many(
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
    ] == TestSupportForComparisonSignsController.get(
        {
            "int_value": [
                (ComparisonSigns.GreaterOrEqual, 122),
                (ComparisonSigns.Lower, 124),
                125,
                126,
            ]
        }
    )


def test_query_with_int_and_less_than_sign_in_int_column_returns_tuple(client, db):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?int_value=<1")
    args = query_get_parser.parse_args()
    assert args["int_value"] == [(ComparisonSigns.Lower, 1)]


def test_query_with_int_and_greater_than_sign_in_int_column_returns_tuple(client, db):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?int_value=>1")
    args = query_get_parser.parse_args()
    assert args["int_value"] == [(ComparisonSigns.Greater, 1)]


def test_query_with_int_and_less_than_or_equal_sign_in_int_column_returns_tuple(
    client, db
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?int_value=<=1")
    args = query_get_parser.parse_args()
    assert args["int_value"] == [(ComparisonSigns.LowerOrEqual, 1)]


def test_query_with_int_and_greater_than_or_equal_sign_in_int_column_returns_tuple(
    client, db
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?int_value=>=1")
    args = query_get_parser.parse_args()
    assert args["int_value"] == [(ComparisonSigns.GreaterOrEqual, 1)]


def test_query_with_float_and_less_than_sign_in_float_column_returns_tuple(client, db):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?float_value=<0.9")
    args = query_get_parser.parse_args()
    assert args["float_value"] == [(ComparisonSigns.Lower, 0.9)]


def test_query_with_float_and_greater_than_sign_in_float_column_returns_tuple(
    client, db
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?float_value=>0.9")
    args = query_get_parser.parse_args()
    assert args["float_value"] == [(ComparisonSigns.Greater, 0.9)]


def test_query_with_float_and_less_than_or_equal_sign_in_float_column_returns_tuple(
    client, db
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?float_value=<=0.9")
    args = query_get_parser.parse_args()
    assert args["float_value"] == [(ComparisonSigns.LowerOrEqual, 0.9)]


def test_query_with_float_and_greater_than_or_equal_sign_in_float_column_returns_tuple(
    client, db
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?float_value=>=0.9")
    args = query_get_parser.parse_args()
    assert args["float_value"] == [(ComparisonSigns.GreaterOrEqual, 0.9)]


def test_query_with_date_and_less_than_sign_in_date_column_returns_tuple(client, db):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?date_value=<2019-01-01")
    args = query_get_parser.parse_args()
    assert args["date_value"] == [
        (
            ComparisonSigns.Lower,
            datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        )
    ]


def test_query_with_date_and_greater_than_sign_in_date_column_returns_tuple(client, db):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?date_value=>2019-01-01")
    args = query_get_parser.parse_args()
    assert args["date_value"] == [
        (
            ComparisonSigns.Greater,
            datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        )
    ]


def test_query_with_date_and_less_than_or_equal_sign_in_date_column_returns_tuple(
    client, db
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?date_value=<=2019-01-01")
    args = query_get_parser.parse_args()
    assert args["date_value"] == [
        (
            ComparisonSigns.LowerOrEqual,
            datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        )
    ]


def test_query_with_date_and_greater_than_or_equal_sign_in_date_column_returns_tuple(
    client, db
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?date_value=>=2019-01-01")
    args = query_get_parser.parse_args()
    assert args["date_value"] == [
        (
            ComparisonSigns.GreaterOrEqual,
            datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        )
    ]


def test_query_with_datetime_and_less_than_sign_in_datetime_column_returns_tuple(
    client, db
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?datetime_value=<2019-01-02T23:59:59")
    args = query_get_parser.parse_args()
    assert args["datetime_value"] == [
        (
            ComparisonSigns.Lower,
            datetime.datetime(2019, 1, 2, 23, 59, 59, tzinfo=datetime.timezone.utc),
        )
    ]


def test_query_with_datetime_and_greater_than_sign_in_datetime_column_returns_tuple(
    client, db
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?datetime_value=>2019-01-02T23:59:59")
    args = query_get_parser.parse_args()
    assert args["datetime_value"] == [
        (
            ComparisonSigns.Greater,
            datetime.datetime(2019, 1, 2, 23, 59, 59, tzinfo=datetime.timezone.utc),
        )
    ]


def test_query_with_datetime_and_less_than_or_equal_sign_in_datetime_column_returns_tuple(
    client, db
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?datetime_value=<=2019-01-02T23:59:59")
    args = query_get_parser.parse_args()
    assert args["datetime_value"] == [
        (
            ComparisonSigns.LowerOrEqual,
            datetime.datetime(2019, 1, 2, 23, 59, 59, tzinfo=datetime.timezone.utc),
        )
    ]


def test_query_with_datetime_and_greater_than_or_equal_sign_in_datetime_column_returns_tuple(
    client, db
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?datetime_value=>=2019-01-02T23:59:59")
    args = query_get_parser.parse_args()
    assert args["datetime_value"] == [
        (
            ComparisonSigns.GreaterOrEqual,
            datetime.datetime(2019, 1, 2, 23, 59, 59, tzinfo=datetime.timezone.utc),
        )
    ]


def test_query_with_int_range_using_comparison_signs_in_int_column_returns_list_of_tuples(
    db, client
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?int_value=>=122&int_value=<124")
    args = query_get_parser.parse_args()
    assert args["int_value"] == [
        (ComparisonSigns.GreaterOrEqual, 122),
        (ComparisonSigns.Lower, 124),
    ]


def test_query_with_float_range_using_comparison_signs_in_float_column_returns_list_of_tuples(
    db, client
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?float_value=>0.9&float_value=<=1.1")
    args = query_get_parser.parse_args()
    assert args["float_value"] == [
        (ComparisonSigns.Greater, 0.9),
        (ComparisonSigns.LowerOrEqual, 1.1),
    ]


def test_query_with_date_range_using_comparison_signs_in_date_column_returns_list_of_tuples(
    db, client
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?date_value=>2019-01-01&date_value=<2019-01-03")
    args = query_get_parser.parse_args()
    assert args["date_value"] == [
        (
            ComparisonSigns.Greater,
            datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        ),
        (
            ComparisonSigns.Lower,
            datetime.datetime(2019, 1, 3, 0, 0, 0, tzinfo=datetime.timezone.utc),
        ),
    ]


def test_query_with_datetime_range_using_comparison_signs_in_datetime_column_returns_list_of_tuples(
    db, client
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get(
        "/test?datetime_value=>=2019-01-01T23:59:59&datetime_value=<=2019-01-03T23:59:59"
    )
    args = query_get_parser.parse_args()
    assert args["datetime_value"] == [
        (
            ComparisonSigns.GreaterOrEqual,
            datetime.datetime(2019, 1, 1, 23, 59, 59, tzinfo=datetime.timezone.utc),
        ),
        (
            ComparisonSigns.LowerOrEqual,
            datetime.datetime(2019, 1, 3, 23, 59, 59, tzinfo=datetime.timezone.utc),
        ),
    ]


def test_query_with_int_range_and_value_out_of_range_using_comparison_signs_in_int_column_returns_list_of_tuples_and_values(
    db, client
):
    query_get_parser = TestSupportForComparisonSignsController.query_get_parser
    client.get("/test?int_value=>=122&int_value=<124&int_value=125")
    args = query_get_parser.parse_args()
    assert args["int_value"] == [
        (ComparisonSigns.GreaterOrEqual, 122),
        (ComparisonSigns.Lower, 124),
        125,
    ]


def test_swagger_content_is_as_expected(client):
    response = client.get("/swagger.json")
    assert response.status_code == 200
    expected = {
        "basePath": "/",
        "consumes": ["application/json"],
        "info": {"title": "API", "version": "1.0"},
        "paths": {
            "/TestNs/test": {
                "get": {
                    "operationId": "get_api_test",
                    "parameters": [
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "date_value",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "datetime_value",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "float_value",
                            "type": "array",
                        },
                        {
                            "collectionFormat": "multi",
                            "in": "query",
                            "items": {"type": "string"},
                            "name": "int_value",
                            "type": "array",
                        },
                        {
                            "exclusiveMinimum": True,
                            "in": "query",
                            "minimum": 0,
                            "name": "limit",
                            "type": "integer",
                        },
                        {
                            "in": "query",
                            "minimum": 0,
                            "name": "offset",
                            "type": "integer",
                        },
                    ],
                    "responses": {"200": {"description": "Success"}},
                    "tags": ["TestNs"],
                }
            }
        },
        "produces": ["application/json"],
        "responses": {
            "MaskError": {"description": "When any error occurs on mask"},
            "ParseError": {"description": "When a mask can't be parsed"},
        },
        "swagger": "2.0",
        "tags": [{"name": "TestNs"}],
    }
    assert response.json == expected
