import datetime
from threading import Thread

import pytest
import sqlalchemy
from flask_restplus import inputs

from layabase import database, database_sqlalchemy
from test.flask_restplus_mock import TestAPI


def parser_types(flask_parser) -> dict:
    return {arg.name: arg.type for arg in flask_parser.args}


class TestController(database.CRUDController):
    pass


class TestAutoIncrementController(database.CRUDController):
    pass


class TestDateController(database.CRUDController):
    pass


class TestInheritanceController(database.CRUDController):
    pass


class TestLikeOperatorController(database.CRUDController):
    pass


def _create_models(base):
    class TestModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "sample_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    class TestAutoIncrementModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "auto_increment_table_name"

        key = sqlalchemy.Column(
            sqlalchemy.Integer, primary_key=True, autoincrement=True
        )
        enum_field = sqlalchemy.Column(
            sqlalchemy.Enum("Value1", "Value2"),
            nullable=False,
            doc="Test Documentation",
        )
        optional_with_default = sqlalchemy.Column(
            sqlalchemy.String, default="Test value"
        )

    class TestDateModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "date_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        date_str = sqlalchemy.Column(sqlalchemy.Date)
        datetime_str = sqlalchemy.Column(sqlalchemy.DateTime)

    class Inherited:
        optional = sqlalchemy.Column(sqlalchemy.String)

    class TestInheritanceModel(database_sqlalchemy.CRUDModel, Inherited, base):
        __tablename__ = "inheritance_table_name"

        key = sqlalchemy.Column(
            sqlalchemy.Integer, primary_key=True, autoincrement=True
        )
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)

    TestInheritanceModel.audit()

    class TestLikeOperatorModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "like_operator_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    TestLikeOperatorModel.interpret_star_character_as_like()

    TestController.model(TestModel)
    TestController.namespace(TestAPI)
    TestAutoIncrementController.model(TestAutoIncrementModel)
    TestAutoIncrementController.namespace(TestAPI)
    TestDateController.model(TestDateModel)
    TestDateController.namespace(TestAPI)
    TestInheritanceController.model(TestInheritanceModel)
    TestInheritanceController.namespace(TestAPI)
    TestLikeOperatorController.model(TestLikeOperatorModel)
    TestLikeOperatorController.namespace(TestAPI)
    return [
        TestModel,
        TestAutoIncrementModel,
        TestDateModel,
        TestInheritanceModel,
        TestLikeOperatorModel,
    ]


@pytest.fixture
def db():
    _db = database.load("sqlite:///:memory:", _create_models)
    yield _db
    database.reset(_db)


def test_get_all_without_data_returns_empty_list(db):
    assert [] == TestController.get({})


def test_post_with_nothing_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_post_list_with_nothing_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_post_with_empty_dict_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_post_many_with_empty_list_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many([])
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_put_with_nothing_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.put(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_put_with_empty_dict_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.put({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_delete_without_nothing_do_not_fail(db):
    assert 0 == TestController.delete({})


def test_post_without_mandatory_field_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post({"key": "my_key"})
    assert {
        "mandatory": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "my_key"} == exception_info.value.received_data


def test_post_many_without_mandatory_field_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many([{"key": "my_key"}])
    assert {
        0: {"mandatory": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [{"key": "my_key"}] == exception_info.value.received_data


def test_post_without_key_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post({"mandatory": 1})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {"mandatory": 1} == exception_info.value.received_data


def test_post_many_without_key_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many([{"mandatory": 1}])
    assert {
        0: {"key": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [{"mandatory": 1}] == exception_info.value.received_data


def test_post_with_wrong_type_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post({"key": 256, "mandatory": 1})
    assert {"key": ["Not a valid string."]} == exception_info.value.errors
    assert {"key": 256, "mandatory": 1} == exception_info.value.received_data


def test_post_many_with_wrong_type_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many([{"key": 256, "mandatory": 1}])
    assert {0: {"key": ["Not a valid string."]}} == exception_info.value.errors
    assert [{"key": 256, "mandatory": 1}] == exception_info.value.received_data


def test_put_with_wrong_type_is_invalid(db):
    TestController.post({"key": "value1", "mandatory": 1})
    with pytest.raises(Exception) as exception_info:
        TestController.put({"key": "value1", "mandatory": "invalid value"})
    assert {"mandatory": ["Not a valid integer."]} == exception_info.value.errors
    assert {
        "key": "value1",
        "mandatory": "invalid value",
    } == exception_info.value.received_data


def test_post_without_optional_is_valid(db):
    assert {"mandatory": 1, "key": "my_key", "optional": None} == TestController.post(
        {"key": "my_key", "mandatory": 1}
    )


def test_post_many_without_optional_is_valid(db):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": None},
        {"mandatory": 2, "key": "my_key2", "optional": None},
    ] == TestController.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )


def test_get_like_operator_double_star(db):
    TestLikeOperatorController.post_many(
        [{"key": "my_key"}, {"key": "my_key2"}, {"key": "my_ey"}]
    )
    assert [{"key": "my_key"}, {"key": "my_key2"}] == TestLikeOperatorController.get(
        {"key": "*y_k*"}
    )


def test_get_like_operator_star_at_start(db):
    TestLikeOperatorController.post_many(
        [{"key": "my_key"}, {"key": "my_key2"}, {"key": "my_ey"}, {"key": "my_k"}]
    )
    assert [{"key": "my_k"}] == TestLikeOperatorController.get({"key": "*y_k"})


def test_get_like_operator_star_at_end(db):
    TestLikeOperatorController.post_many(
        [
            {"key": "my_key"},
            {"key": "my_key2"},
            {"key": "my_ey"},
            {"key": "my_k"},
            {"key": "y_key"},
        ]
    )
    assert [{"key": "y_key"}] == TestLikeOperatorController.get({"key": "y_k*"})


def test_get_like_operator_no_star(db):
    TestLikeOperatorController.post_many(
        [
            {"key": "my_key"},
            {"key": "my_key2"},
            {"key": "my_ey"},
            {"key": "my_k"},
            {"key": "y_key"},
        ]
    )
    assert [{"key": "my_key"}] == TestLikeOperatorController.get({"key": "my_key"})


def test_get_like_operator_no_star_no_result(db):
    TestLikeOperatorController.post_many(
        [
            {"key": "my_key"},
            {"key": "my_key2"},
            {"key": "my_ey"},
            {"key": "my_k"},
            {"key": "y_key"},
        ]
    )
    assert [] == TestLikeOperatorController.get({"key": "y_k"})


def test_get_no_like_operator(db):
    TestController.post_many(
        [
            {"key": "my_key", "mandatory": 1},
            {"key": "my_key2", "mandatory": 1},
            {"key": "my_ey", "mandatory": 1},
            {"key": "my_k", "mandatory": 1},
            {"key": "y_key", "mandatory": 1},
        ]
    )
    assert [] == TestController.get({"key": "*y_k*"})


def test_post_with_optional_is_valid(db):
    assert {
        "mandatory": 1,
        "key": "my_key",
        "optional": "my_value",
    } == TestController.post({"key": "my_key", "mandatory": 1, "optional": "my_value"})


def test_post_many_with_optional_is_valid(db):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": "my_value"},
        {"mandatory": 2, "key": "my_key2", "optional": "my_value2"},
    ] == TestController.post_many(
        [
            {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )


def test_post_with_unknown_field_is_valid(db):
    assert {
        "mandatory": 1,
        "key": "my_key",
        "optional": "my_value",
    } == TestController.post(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    )


def test_post_many_with_unknown_field_is_valid(db):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": "my_value"},
        {"mandatory": 2, "key": "my_key2", "optional": "my_value2"},
    ] == TestController.post_many(
        [
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            },
            {
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                # This field do not exists in schema
                "unknown": "my_value2",
            },
        ]
    )


def test_post_with_specified_incremented_field_is_ignored_and_valid(db):
    assert {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    } == TestAutoIncrementController.post({"key": "my_key", "enum_field": "Value1"})


def test_post_many_with_specified_incremented_field_is_ignored_and_valid(db):
    assert [
        {"optional_with_default": "Test value", "enum_field": "Value1", "key": 1},
        {"optional_with_default": "Test value", "enum_field": "Value2", "key": 2},
    ] == TestAutoIncrementController.post_many(
        [
            {"key": "my_key", "enum_field": "Value1"},
            {"key": "my_key", "enum_field": "Value2"},
        ]
    )


def test_get_without_filter_is_retrieving_the_only_item(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert [
        {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
    ] == TestController.get({})


def test_get_from_another_thread_than_post(db):
    def save_get_result():
        assert [
            {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
        ] == TestController.get({})

    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})

    get_thread = Thread(name="GetInOtherThread", target=save_get_result)
    get_thread.start()
    get_thread.join()


def test_get_without_filter_is_retrieving_everything_with_multiple_posts(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({})


def test_get_without_filter_is_retrieving_everything(db):
    TestController.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({})


def test_get_with_filter_is_retrieving_subset_with_multiple_posts(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == TestController.get({"optional": "my_value1"})


def test_get_with_list_filter_matching_one_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == TestController.get({"optional": ["my_value1"]})


def test_get_with_list_filter_matching_many_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({"optional": ["my_value1", "my_value2"]})


def test_get_with_list_filter_matching_partial_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == TestController.get({"optional": ["non existing", "my_value1", "not existing"]})


def test_get_with_empty_list_filter_is_retrieving_everything(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({"optional": []})


def test_delete_with_list_filter_matching_one_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 1 == TestController.delete({"optional": ["my_value1"]})


def test_delete_with_list_filter_matching_many_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == TestController.delete({"optional": ["my_value1", "my_value2"]})


def test_delete_with_list_filter_matching_partial_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 1 == TestController.delete(
        {"optional": ["non existing", "my_value1", "not existing"]}
    )


def test_delete_with_empty_list_filter_is_retrieving_everything(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == TestController.delete({"optional": []})


def test_get_with_filter_is_retrieving_subset(db):
    TestController.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == TestController.get({"optional": "my_value1"})


def test_put_is_updating(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    ) == TestController.put({"key": "my_key1", "optional": "my_value"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ] == TestController.get({"mandatory": 1})


def test_put_is_updating_date(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert (
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        },
        {
            "date_str": "2018-06-01",
            "datetime_str": "1989-12-31T01:00:00",
            "key": "my_key1",
        },
    ) == TestDateController.put(
        {
            "key": "my_key1",
            "date_str": "2018-06-01",
            "datetime_str": "1989-12-31T01:00:00",
        }
    )
    assert [
        {
            "date_str": "2018-06-01",
            "datetime_str": "1989-12-31T01:00:00",
            "key": "my_key1",
        }
    ] == TestDateController.get({"date_str": "2018-06-01"})


def test_get_date_is_handled_for_valid_date(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ] == TestDateController.get({"date_str": datetime.date(2017, 5, 15)})


def test_get_date_is_handled_for_unused_date(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    d = datetime.datetime.strptime("2016-09-23", "%Y-%m-%d").date()
    assert [] == TestDateController.get({"date_str": d})


def test_get_date_is_handled_for_valid_datetime(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ] == TestDateController.get({"datetime_str": datetime.datetime(2016, 9, 23, 23, 59, 59)})


def test_get_date_is_handled_for_unused_datetime(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    dt = datetime.datetime.strptime("2016-09-24T23:59:59", "%Y-%m-%dT%H:%M:%S")
    assert [] == TestDateController.get({"datetime_str": dt})


def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.put({"key": "my_key1", "optional": "my_value"})
    assert [] == TestController.get({"optional": "my_value1"})


def test_delete_with_filter_is_removing_the_proper_row(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 1 == TestController.delete({"key": "my_key1"})
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ] == TestController.get({})


def test_delete_without_filter_is_removing_everything(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == TestController.delete({})
    assert [] == TestController.get({})


def test_query_get_parser(db):
    assert {
        "key": str,
        "mandatory": int,
        "optional": str,
        "limit": inputs.positive,
        "order_by": str,
        "offset": inputs.natural,
    } == parser_types(TestController.query_get_parser)


def test_query_delete_parser(db):
    assert {"key": str, "mandatory": int, "optional": str} == parser_types(
        TestController.query_delete_parser
    )


def test_json_post_model(db):
    assert "TestModel" == TestController.json_post_model.name
    assert {
        "key": "String",
        "mandatory": "Integer",
        "optional": "String",
    } == TestController.json_post_model.fields_flask_type


def test_json_post_model_with_auto_increment_and_enum(db):
    assert "TestAutoIncrementModel" == TestAutoIncrementController.json_post_model.name
    assert {
        "enum_field": "String",
        "key": "Integer",
        "optional_with_default": "String",
    } == TestAutoIncrementController.json_post_model.fields_flask_type
    assert {
        "enum_field": None,
        "key": None,
        "optional_with_default": "Test value",
    } == TestAutoIncrementController.json_post_model.fields_default


def test_json_put_model(db):
    assert "TestModel" == TestController.json_put_model.name
    assert {
        "key": "String",
        "mandatory": "Integer",
        "optional": "String",
    } == TestController.json_put_model.fields_flask_type


def test_json_put_model_with_auto_increment_and_enum(db):
    assert "TestAutoIncrementModel" == TestAutoIncrementController.json_put_model.name
    assert {
        "enum_field": "String",
        "key": "Integer",
        "optional_with_default": "String",
    } == TestAutoIncrementController.json_put_model.fields_flask_type


def test_get_response_model(db):
    assert "TestModel" == TestController.get_response_model.name
    assert {
        "key": "String",
        "mandatory": "Integer",
        "optional": "String",
    } == TestController.get_response_model.fields_flask_type


def test_get_response_model_with_enum(db):
    assert (
        "TestAutoIncrementModel" == TestAutoIncrementController.get_response_model.name
    )
    assert {
        "enum_field": "String",
        "key": "Integer",
        "optional_with_default": "String",
    } == TestAutoIncrementController.get_response_model.fields_flask_type
    assert {
        "enum_field": "Test Documentation",
        "key": None,
        "optional_with_default": None,
    } == TestAutoIncrementController.get_response_model.fields_description
    assert {
        "enum_field": ["Value1", "Value2"],
        "key": None,
        "optional_with_default": None,
    } == TestAutoIncrementController.get_response_model.fields_enum


def test_get_with_order_by_desc_is_retrieving_elements_ordered_by_descending_mode(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert [
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
    ] == TestController.get({"order_by": ["key desc"]})


def test_get_with_order_by_is_retrieving_elements_ordered_by_ascending_mode(db):
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ] == TestController.get({"order_by": ["key"]})


def test_get_with_2_order_by_is_retrieving_elements_ordered_by(db):
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ] == TestController.get({"order_by": ["key", "mandatory desc"]})


def test_get_with_limit_2_is_retrieving_subset_of_2_first_elements(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({"limit": 2})


def test_get_with_offset_1_is_retrieving_subset_of_n_minus_1_first_elements(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ] == TestController.get({"offset": 1})


def test_get_with_limit_1_and_offset_1_is_retrieving_middle_element(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ] == TestController.get({"offset": 1, "limit": 1})


def test_get_model_description_returns_description(db):
    assert {
        "key": "key",
        "mandatory": "mandatory",
        "optional": "optional",
        "table": "sample_table_name",
    } == TestController.get_model_description()


def test_get_model_description_response_model(db):
    assert (
        "TestModelDescription"
        == TestController.get_model_description_response_model.name
    )
    assert {
        "key": "String",
        "mandatory": "String",
        "optional": "String",
        "table": "String",
    } == TestController.get_model_description_response_model.fields_flask_type
