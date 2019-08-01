import datetime
import enum
import json
import re
from threading import Thread

import pytest
from flask_restplus import inputs
from pycommon_error.validation import ValidationFailed

from layabase import database, database_mongo, versioning_mongo
from test.flask_restplus_mock import TestAPI


def parser_types(flask_parser) -> dict:
    return {arg.name: arg.type for arg in flask_parser.args}


def parser_actions(flask_parser) -> dict:
    return {arg.name: arg.action for arg in flask_parser.args}


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestController(database.CRUDController):
    pass


class TestStrictController(database.CRUDController):
    pass


class TestAutoIncrementController(database.CRUDController):
    pass


class TestDateController(database.CRUDController):
    pass


class TestDictController(database.CRUDController):
    pass


class TestOptionalDictController(database.CRUDController):
    pass


class TestIndexController(database.CRUDController):
    pass


class TestDefaultPrimaryKeyController(database.CRUDController):
    pass


class TestListController(database.CRUDController):
    pass


class TestStringListController(database.CRUDController):
    pass


class TestLimitsController(database.CRUDController):
    pass


class TestIdController(database.CRUDController):
    pass


class TestUnvalidatedListAndDictController(database.CRUDController):
    pass


class TestVersionedController(database.CRUDController):
    pass


class TestNullableAutoSetController(database.CRUDController):
    pass


class TestVersionedUniqueNonPrimaryController(database.CRUDController):
    pass


class TestUniqueNonPrimaryController(database.CRUDController):
    pass


class TestIntAndFloatController(database.CRUDController):
    pass


class TestDictInDictController(database.CRUDController):
    pass


class TestNoneInsertController(database.CRUDController):
    pass


class TestNoneRetrieveController(database.CRUDController):
    pass


class TestNoneNotInsertedController(database.CRUDController):
    pass


class TestDictRequiredNonNullableVersionedController(database.CRUDController):
    pass


def _create_models(base):
    class TestModel(
        database_mongo.CRUDModel, base=base, table_name="sample_table_name"
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        mandatory = database_mongo.Column(int, is_nullable=False)
        optional = database_mongo.Column(str)

    class TestStrictModel(
        database_mongo.CRUDModel,
        base=base,
        table_name="strict_table_name",
        skip_unknown_fields=False,
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        mandatory = database_mongo.Column(int, is_nullable=False)
        optional = database_mongo.Column(str)

    class TestAutoIncrementModel(
        database_mongo.CRUDModel, base=base, table_name="auto_increment_table_name"
    ):
        key = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        enum_field = database_mongo.Column(
            EnumTest, is_nullable=False, description="Test Documentation"
        )
        optional_with_default = database_mongo.Column(str, default_value="Test value")

    class TestDateModel(
        database_mongo.CRUDModel, base=base, table_name="date_table_name"
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        date_str = database_mongo.Column(datetime.date)
        datetime_str = database_mongo.Column(datetime.datetime)

    class TestDictModel(
        database_mongo.CRUDModel, base=base, table_name="dict_table_name"
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        dict_col = database_mongo.DictColumn(
            fields={
                "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                "second_key": database_mongo.Column(int, is_nullable=False),
            },
            is_nullable=False,
        )

    class TestOptionalDictModel(
        database_mongo.CRUDModel, base=base, table_name="optional_dict_table_name"
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        dict_col = database_mongo.DictColumn(
            get_fields=lambda model_as_dict: {
                "first_key": database_mongo.Column(EnumTest, is_nullable=True),
                "second_key": database_mongo.Column(int, is_nullable=True),
            }
        )

    class TestIndexModel(
        database_mongo.CRUDModel, base=base, table_name="index_table_name"
    ):
        unique_key = database_mongo.Column(str, is_primary_key=True)
        non_unique_key = database_mongo.Column(
            datetime.date, index_type=database_mongo.IndexType.Other
        )

    class TestDefaultPrimaryKeyModel(
        database_mongo.CRUDModel, base=base, table_name="default_primary_table_name"
    ):
        key = database_mongo.Column(is_primary_key=True, default_value="test")
        optional = database_mongo.Column()

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

    class TestStringListModel(
        database_mongo.CRUDModel, base=base, table_name="string_list_table_name"
    ):
        key = database_mongo.Column(is_primary_key=True)
        list_field = database_mongo.ListColumn(database_mongo.Column(), sorted=True)

    class TestLimitsModel(
        database_mongo.CRUDModel, base=base, table_name="limits_table_name"
    ):
        key = database_mongo.Column(is_primary_key=True, min_length=3, max_length=4)
        list_field = database_mongo.Column(
            list, min_length=2, max_length=3, example=["my", "test"]
        )
        int_field = database_mongo.Column(int, min_value=100, max_value=999)

    class TestUnvalidatedListAndDictModel(
        database_mongo.CRUDModel, base=base, table_name="list_and_dict_table_name"
    ):
        float_key = database_mongo.Column(float, is_primary_key=True)
        float_with_default = database_mongo.Column(float, default_value=34)
        dict_field = database_mongo.Column(dict, is_required=True)
        list_field = database_mongo.Column(list, is_required=True)

    class TestIdModel(database_mongo.CRUDModel, base=base, table_name="id_table_name"):
        _id = database_mongo.Column(is_primary_key=True)

    class TestNullableAutoSetModel(
        database_mongo.CRUDModel, base=base, table_name="nullable_auto_set_table_name"
    ):
        prim_def_inc = database_mongo.Column(
            int, is_primary_key=True, default_value=1, should_auto_increment=True
        )
        prim_def = database_mongo.Column(int, is_primary_key=True, default_value=1)
        prim_inc = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )

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

    class TestDictRequiredNonNullableVersionedModel(
        versioning_mongo.VersionedCRUDModel,
        base=base,
        table_name="req_not_null_versioned_table_name",
    ):
        key = database_mongo.Column(is_primary_key=True)
        dict_field = database_mongo.DictColumn(
            fields={
                "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                "second_key": database_mongo.Column(int, is_nullable=False),
            },
            is_required=True,
            is_nullable=False,
        )

    class TestVersionedUniqueNonPrimaryModel(
        versioning_mongo.VersionedCRUDModel,
        base=base,
        table_name="versioned_uni_table_name",
    ):
        key = database_mongo.Column(int, should_auto_increment=True)
        unique = database_mongo.Column(int, index_type=database_mongo.IndexType.Unique)

    class TestUniqueNonPrimaryModel(
        database_mongo.CRUDModel, base=base, table_name="uni_table_name"
    ):
        key = database_mongo.Column(int, should_auto_increment=True)
        unique = database_mongo.Column(int, index_type=database_mongo.IndexType.Unique)

    class TestIntAndFloatModel(
        database_mongo.CRUDModel, base=base, table_name="int_and_float"
    ):
        int_value = database_mongo.Column(int)
        float_value = database_mongo.Column(float)

    class TestDictInDictModel(
        database_mongo.CRUDModel, base=base, table_name="dict_in_dict_table_name"
    ):
        key = database_mongo.Column(is_primary_key=True)
        dict_field = database_mongo.DictColumn(
            fields={
                "first_key": database_mongo.DictColumn(
                    fields={
                        "inner_key1": database_mongo.Column(
                            EnumTest, is_nullable=False
                        ),
                        "inner_key2": database_mongo.Column(int, is_nullable=False),
                    },
                    is_required=True,
                ),
                "second_key": database_mongo.Column(int, is_nullable=False),
            },
            is_required=True,
        )

    class TestNoneNotInsertedModel(
        database_mongo.CRUDModel, base=base, table_name="none_table_name"
    ):
        key = database_mongo.Column(int, is_primary_key=True)
        my_dict = database_mongo.DictColumn(
            fields={"null_value": database_mongo.Column(store_none=False)},
            is_required=True,
        )

    class TestNoneInsertModel(
        database_mongo.CRUDModel,
        base=base,
        table_name="none_table_name",
        skip_name_check=True,
    ):
        key = database_mongo.Column(int, is_primary_key=True)
        my_dict = database_mongo.DictColumn(
            fields={"null_value": database_mongo.Column(store_none=True)},
            is_required=True,
        )

    class TestNoneRetrieveModel(
        database_mongo.CRUDModel,
        base=base,
        table_name="none_table_name",
        skip_name_check=True,
    ):
        key = database_mongo.Column(int, is_primary_key=True)
        my_dict = database_mongo.Column(dict, is_required=True)

    TestController.model(TestModel)
    TestStrictController.model(TestStrictModel)
    TestAutoIncrementController.model(TestAutoIncrementModel)
    TestDateController.model(TestDateModel)
    TestDictController.model(TestDictModel)
    TestOptionalDictController.model(TestOptionalDictModel)
    TestIndexController.model(TestIndexModel)
    TestDefaultPrimaryKeyController.model(TestDefaultPrimaryKeyModel)
    TestListController.model(TestListModel)
    TestStringListController.model(TestStringListModel)
    TestLimitsController.model(TestLimitsModel)
    TestIdController.model(TestIdModel)
    TestUnvalidatedListAndDictController.model(TestUnvalidatedListAndDictModel)
    TestNullableAutoSetController.model(TestNullableAutoSetModel)
    TestVersionedController.model(TestVersionedModel)
    TestDictRequiredNonNullableVersionedController.model(
        TestDictRequiredNonNullableVersionedModel
    )
    TestVersionedUniqueNonPrimaryController.model(TestVersionedUniqueNonPrimaryModel)
    TestUniqueNonPrimaryController.model(TestUniqueNonPrimaryModel)
    TestIntAndFloatController.model(TestIntAndFloatModel)
    TestDictInDictController.model(TestDictInDictModel)
    TestNoneNotInsertedController.model(TestNoneNotInsertedModel)
    TestNoneInsertController.model(TestNoneInsertModel)
    TestNoneRetrieveController.model(TestNoneRetrieveModel)
    return [
        TestModel,
        TestStrictModel,
        TestAutoIncrementModel,
        TestDateModel,
        TestDictModel,
        TestOptionalDictModel,
        TestIndexModel,
        TestDefaultPrimaryKeyModel,
        TestListModel,
        TestStringListModel,
        TestLimitsModel,
        TestIdModel,
        TestUnvalidatedListAndDictModel,
        TestVersionedModel,
        TestDictRequiredNonNullableVersionedModel,
        TestNullableAutoSetModel,
        TestVersionedUniqueNonPrimaryModel,
        TestUniqueNonPrimaryModel,
        TestIntAndFloatModel,
        TestDictInDictModel,
        TestNoneInsertModel,
    ]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    TestController.namespace(TestAPI)
    TestStrictController.namespace(TestAPI)
    TestAutoIncrementController.namespace(TestAPI)
    TestDateController.namespace(TestAPI)
    TestDictController.namespace(TestAPI)
    TestOptionalDictController.namespace(TestAPI)
    TestIndexController.namespace(TestAPI)
    TestDefaultPrimaryKeyController.namespace(TestAPI)
    TestListController.namespace(TestAPI)
    TestStringListController.namespace(TestAPI)
    TestLimitsController.namespace(TestAPI)
    TestIdController.namespace(TestAPI)
    TestUnvalidatedListAndDictController.namespace(TestAPI)
    TestVersionedController.namespace(TestAPI)
    TestDictRequiredNonNullableVersionedController.namespace(TestAPI)
    TestNullableAutoSetController.namespace(TestAPI)
    TestVersionedUniqueNonPrimaryController.namespace(TestAPI)
    TestUniqueNonPrimaryController.namespace(TestAPI)
    TestIntAndFloatController.namespace(TestAPI)
    TestDictInDictController.namespace(TestAPI)
    TestNoneInsertController.namespace(TestAPI)
    TestNoneRetrieveController.namespace(TestAPI)
    TestNoneNotInsertedController.namespace(TestAPI)

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


def test_health_details_failure(db, monkeypatch):
    monkeypatch.setattr(database_mongo, "datetime", DateTimeModuleMock)

    def fail_ping(*args):
        raise Exception("Unable to ping")

    db.command = fail_ping
    assert database.health_details(db) == (
        "fail",
        {
            "mongomock:ping": {
                "componentType": "datastore",
                "output": "Unable to ping",
                "status": "fail",
                "time": "2018-10-11T15:05:05.663979",
            }
        },
    )


def test_health_details_success(db, monkeypatch):
    monkeypatch.setattr(database_mongo, "datetime", DateTimeModuleMock)
    assert database.health_details(db) == (
        "pass",
        {
            "mongomock:ping": {
                "componentType": "datastore",
                "observedValue": {"ok": 1.0},
                "status": "pass",
                "time": "2018-10-11T15:05:05.663979",
            }
        },
    )


def test_get_all_without_data_returns_empty_list(db):
    assert TestController.get({}) == []


def test_post_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert not exception_info.value.received_data


def test_post_list_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == []


def test_post_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({})
    assert exception_info.value.errors == {
        "key": ["Missing data for required field."],
        "mandatory": ["Missing data for required field."],
    }
    assert exception_info.value.received_data == {}


def test_post_with_list_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post([""])
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert [""] == exception_info.value.received_data


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


def test_get_url_without_primary_key_in_model_and_many_models(db):
    models = [{"key": 1, "unique": 2}, {"key": 2, "unique": 3}]
    assert TestUniqueNonPrimaryController.get_url("/test", *models) == "/test"


def test_get_url_without_primary_key_in_model_and_one_model(db):
    model = {"key": 1, "unique": 2}
    assert TestUniqueNonPrimaryController.get_url("/test", model) == "/test"


def test_get_url_without_primary_key_in_model_and_no_model(db):
    assert "/test" == TestUniqueNonPrimaryController.get_url("/test")


def test_post_many_with_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many({""})
    assert {"": ["Must be a list of dictionaries."]} == exception_info.value.errors
    assert {""} == exception_info.value.received_data


def test_put_with_list_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put([""])
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert [""] == exception_info.value.received_data


def test_put_many_with_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put_many({""})
    assert {"": ["Must be a list."]} == exception_info.value.errors
    assert {""} == exception_info.value.received_data


def test_post_many_with_empty_list_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([])
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert [] == exception_info.value.received_data


def test_put_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert None == exception_info.value.received_data


def test_put_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put({})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
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
        TestController.post({"key": datetime.date(2007, 12, 5), "mandatory": 1})
    assert {"key": ["Not a valid str."]} == exception_info.value.errors
    assert {
        "key": datetime.date(2007, 12, 5),
        "mandatory": 1,
    } == exception_info.value.received_data


def test_post_int_instead_of_str_is_valid(db):
    assert {"key": "3", "mandatory": 1, "optional": None} == TestController.post(
        {"key": 3, "mandatory": 1}
    )


def test_post_boolean_instead_of_str_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post({"key": True, "mandatory": 1})
    assert {"key": ["Not a valid str."]} == exception_info.value.errors
    assert {"key": True, "mandatory": 1} == exception_info.value.received_data


def test_post_float_instead_of_str_is_valid(db):
    assert {"key": "1.5", "mandatory": 1, "optional": None} == TestController.post(
        {"key": 1.5, "mandatory": 1}
    )


def test_post_twice_with_unique_index_is_invalid(db):
    assert {
        "non_unique_key": "2017-01-01",
        "unique_key": "test",
    } == TestIndexController.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    with pytest.raises(Exception) as exception_info:
        TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-02"})
    assert {"": ["This document already exists."]} == exception_info.value.errors
    assert {
        "non_unique_key": "2017-01-02",
        "unique_key": "test",
    } == exception_info.value.received_data


def test_get_all_without_primary_key_is_valid(db):
    assert {
        "non_unique_key": "2017-01-01",
        "unique_key": "test",
    } == TestIndexController.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    assert [
        {"non_unique_key": "2017-01-01", "unique_key": "test"}
    ] == TestIndexController.get({})


def test_get_one_and_multiple_results_is_invalid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    with pytest.raises(Exception) as exception_info:
        TestIndexController.get_one({})
    assert {
        "": ["More than one result: Consider another filtering."]
    } == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_get_one_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert {
        "unique_key": "test2",
        "non_unique_key": "2017-01-01",
    } == TestIndexController.get_one({"unique_key": "test2"})


def test_get_with_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert TestIndexController.get({"unique_key": ["test", "test2"]}) == [
        {"unique_key": "test", "non_unique_key": "2017-01-01"},
        {"unique_key": "test2", "non_unique_key": "2017-01-01"},
    ]


def test_get_with_partial_matching_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert TestIndexController.get({"unique_key": ["test2"]}) == [
        {"unique_key": "test2", "non_unique_key": "2017-01-01"}
    ]


def test_get_with_empty_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert TestIndexController.get({"unique_key": []}) == [
        {"unique_key": "test", "non_unique_key": "2017-01-01"},
        {"unique_key": "test2", "non_unique_key": "2017-01-01"},
    ]


def test_get_with_partialy_matching_and_not_matching_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert [
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    ] == TestIndexController.get(
        {"unique_key": ["not existing", "test", "another non existing"]}
    )


def test_post_int_str_in_int_column(db):
    assert {"int_value": 15, "float_value": 1.0} == TestIntAndFloatController.post(
        {"int_value": "15", "float_value": 1.0}
    )


def test_put_int_str_in_int_column(db):
    TestIntAndFloatController.post({"int_value": 15, "float_value": 1.0})
    assert (
        {"int_value": 15, "float_value": 1.0},
        {"int_value": 16, "float_value": 1.0},
    ) == TestIntAndFloatController.put({"int_value": "16", "float_value": 1.0})


def test_put_without_primary_and_incremented_field(db):
    TestNullableAutoSetController.post({"prim_def": 1})
    with pytest.raises(Exception) as exception_info:
        TestNullableAutoSetController.put({"prim_def": 1})
    assert {
        "prim_inc": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"prim_def": 1} == exception_info.value.received_data


def test_delete_int_str_in_int_column(db):
    TestIntAndFloatController.post({"int_value": 15, "float_value": 1.0})
    assert 1 == TestIntAndFloatController.delete({"int_value": "15"})


def test_post_float_str_in_float_column(db):
    assert {"int_value": 15, "float_value": 1.3} == TestIntAndFloatController.post(
        {"int_value": 15, "float_value": "1.3"}
    )


def test_put_float_str_in_float_column(db):
    TestIntAndFloatController.post({"int_value": 15, "float_value": 1.3})
    assert (
        {"int_value": 15, "float_value": 1.3},
        {"int_value": 15, "float_value": 1.4},
    ) == TestIntAndFloatController.put({"int_value": 15, "float_value": "1.4"})


def test_delete_float_str_in_float_column(db):
    TestIntAndFloatController.post({"int_value": 15, "float_value": 1.3})
    assert 1 == TestIntAndFloatController.delete({"float_value": "1.3"})


def test_post_with_non_int_str_in_int_column(db):
    with pytest.raises(Exception) as exception_info:
        TestIntAndFloatController.post({"int_value": "abc", "float_value": 1.0})
    assert {"int_value": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "int_value": "abc",
        "float_value": 1.0,
    } == exception_info.value.received_data


def test_post_with_non_float_str_in_float_column(db):
    with pytest.raises(Exception) as exception_info:
        TestIntAndFloatController.post({"int_value": 1, "float_value": "abc"})
    assert {"float_value": ["Not a valid float."]} == exception_info.value.errors
    assert {"float_value": "abc", "int_value": 1} == exception_info.value.received_data


def test_get_with_non_int_str_in_int_column(db):
    with pytest.raises(Exception) as exception_info:
        TestIntAndFloatController.get({"int_value": "abc", "float_value": 1.0})
    assert {"int_value": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "int_value": "abc",
        "float_value": 1.0,
    } == exception_info.value.received_data


def test_get_with_non_float_str_in_float_column(db):
    with pytest.raises(Exception) as exception_info:
        TestIntAndFloatController.get({"int_value": 1, "float_value": "abc"})
    assert {"float_value": ["Not a valid float."]} == exception_info.value.errors
    assert {"float_value": "abc", "int_value": 1} == exception_info.value.received_data


def test_put_with_non_int_str_in_int_column(db):
    with pytest.raises(Exception) as exception_info:
        TestIntAndFloatController.put({"int_value": "abc", "float_value": 1.0})
    assert {"int_value": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "int_value": "abc",
        "float_value": 1.0,
    } == exception_info.value.received_data


def test_put_with_non_float_str_in_float_column(db):
    with pytest.raises(Exception) as exception_info:
        TestIntAndFloatController.put({"int_value": 1, "float_value": "abc"})
    assert {"float_value": ["Not a valid float."]} == exception_info.value.errors
    assert {"float_value": "abc", "int_value": 1} == exception_info.value.received_data


def test_delete_with_non_int_str_in_int_column(db):
    with pytest.raises(Exception) as exception_info:
        TestIntAndFloatController.delete({"int_value": "abc", "float_value": 1.0})
    assert {"int_value": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "int_value": "abc",
        "float_value": 1.0,
    } == exception_info.value.received_data


def test_delete_with_non_float_str_in_float_column(db):
    with pytest.raises(Exception) as exception_info:
        TestIntAndFloatController.delete({"int_value": 1, "float_value": "abc"})
    assert {"float_value": ["Not a valid float."]} == exception_info.value.errors
    assert {"float_value": "abc", "int_value": 1} == exception_info.value.received_data


def test_get_is_valid_with_int_str_in_int_column(db):
    TestIntAndFloatController.post({"int_value": 123, "float_value": 1.0})
    assert {"int_value": 123, "float_value": 1.0} == TestIntAndFloatController.get_one(
        {"int_value": "123"}
    )


def test_get_retrieve_none_field_when_not_in_model(db):
    TestNoneInsertController.post({"key": 1, "my_dict": {"null_value": None}})
    assert [
        {"key": 1, "my_dict": {"null_value": None}}
    ] == TestNoneRetrieveController.get({})


def test_get_do_not_retrieve_none_field_when_not_in_model(db):
    TestNoneNotInsertedController.post({"key": 1, "my_dict": {"null_value": None}})
    assert [{"key": 1, "my_dict": {}}] == TestNoneRetrieveController.get({})


def test_get_is_valid_with_float_str_in_float_column(db):
    TestIntAndFloatController.post({"int_value": 1, "float_value": 1.23})
    assert {"int_value": 1, "float_value": 1.23} == TestIntAndFloatController.get_one(
        {"float_value": "1.23"}
    )


def test_delete_with_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 2 == TestIndexController.delete({"unique_key": ["test", "test2"]})


def test_delete_with_partial_matching_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 1 == TestIndexController.delete({"unique_key": ["test2"]})


def test_non_iso8601_date_failure(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestIndexController.post({"unique_key": "test", "non_unique_key": "12/06/2017"})
    assert {"non_unique_key": ["Not a valid date."]} == exception_info.value.errors
    assert {
        "unique_key": "test",
        "non_unique_key": "12/06/2017",
    } == exception_info.value.received_data


def test_delete_with_empty_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 2 == TestIndexController.delete({"unique_key": []})


def test_delete_with_partialy_matching_and_not_matching_list_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert 1 == TestIndexController.delete(
        {"unique_key": ["not existing", "test", "another non existing"]}
    )


def test_get_one_without_result_is_valid(db):
    TestIndexController.post({"unique_key": "test", "non_unique_key": "2017-01-01"})
    TestIndexController.post({"unique_key": "test2", "non_unique_key": "2017-01-01"})
    assert {} == TestIndexController.get_one({"unique_key": "test3"})


def test_get_field_names_valid(db):
    assert ["non_unique_key", "unique_key"] == TestIndexController.get_field_names()


def _assert_regex(expected, actual):
    assert re.match(
        f"{expected}".replace("[", "\\[").replace("]", "\\]").replace("\\\\", "\\"),
        f"{actual}",
    )


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


def test_post_without_providing_required_non_nullable_dict_column_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestDictRequiredNonNullableVersionedController.post({"key": "first"})
    assert {
        "dict_field": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "first"} == exception_info.value.received_data


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


def test_put_without_providing_required_non_nullable_dict_column_is_valid(db):
    TestDictRequiredNonNullableVersionedController.post(
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
    ) == TestDictRequiredNonNullableVersionedController.put({"key": "first"})


def test_put_with_null_provided_required_non_nullable_dict_column_is_invalid(db):
    TestDictRequiredNonNullableVersionedController.post(
        {"key": "first", "dict_field": {"first_key": "Value1", "second_key": 0}}
    )
    with pytest.raises(ValidationFailed) as exception_info:
        TestDictRequiredNonNullableVersionedController.put(
            {"key": "first", "dict_field": None}
        )
    assert {
        "dict_field": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "first", "dict_field": None} == exception_info.value.received_data


def test_revison_is_shared(db):
    TestVersionedController.post(
        {
            "key": "first",
            "dict_field.first_key": EnumTest.Value1,
            "dict_field.second_key": 1,
        }
    )
    TestVersionedUniqueNonPrimaryController.post({"unique": 1})
    TestVersionedController.put({"key": "first", "dict_field.second_key": 2})
    TestVersionedController.delete({"key": "first"})
    TestVersionedController.rollback_to({"revision": 2})
    assert [
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 2},
            "valid_since_revision": 3,
            "valid_until_revision": 4,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": 3,
        },
        {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 5,
            "valid_until_revision": -1,
        },
    ] == TestVersionedController.get_history({})
    assert [
        {"key": 1, "unique": 1, "valid_since_revision": 2, "valid_until_revision": -1}
    ] == TestVersionedUniqueNonPrimaryController.get_history({})


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
    with pytest.raises(Exception) as exception_info:
        TestVersionedController.rollback_to({"key": "unknown"})
    assert {
        "revision": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "unknown"} == exception_info.value.received_data


def test_rollback_with_non_int_revision_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestVersionedController.rollback_to({"revision": "invalid revision"})
    assert {"revision": ["Not a valid int."]} == exception_info.value.errors
    assert {"revision": "invalid revision"} == exception_info.value.received_data


def test_rollback_without_versioning_is_valid(db):
    assert 0 == TestController.rollback_to({"revision": "invalid revision"})


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


def test_versioning_handles_unique_non_primary(db):
    TestVersionedUniqueNonPrimaryController.post({"unique": 1})
    with pytest.raises(Exception) as exception_info:
        TestVersionedUniqueNonPrimaryController.post({"unique": 1})
    assert {"": ["This document already exists."]} == exception_info.value.errors
    assert {
        "key": 2,
        "unique": 1,
        "valid_since_revision": 2,
        "valid_until_revision": -1,
    } == exception_info.value.received_data


def test_insert_to_non_unique_after_update(db):
    TestVersionedUniqueNonPrimaryController.post({"unique": 1})
    TestVersionedUniqueNonPrimaryController.put({"key": 1, "unique": 2})
    with pytest.raises(Exception) as exception_info:
        TestVersionedUniqueNonPrimaryController.post({"unique": 2})
    assert {"": ["This document already exists."]} == exception_info.value.errors
    assert {
        "key": 2,
        "unique": 2,
        "valid_since_revision": 3,
        "valid_until_revision": -1,
    } == exception_info.value.received_data


def test_update_to_non_unique_versioned(db):
    TestVersionedUniqueNonPrimaryController.post({"unique": 1})
    TestVersionedUniqueNonPrimaryController.post({"unique": 2})
    with pytest.raises(Exception) as exception_info:
        TestVersionedUniqueNonPrimaryController.put({"key": 1, "unique": 2})
    assert {"": ["This document already exists."]} == exception_info.value.errors
    assert {
        "key": 1,
        "unique": 2,
        "valid_since_revision": 3,
        "valid_until_revision": -1,
    } == exception_info.value.received_data


def test_update_to_non_unique(db):
    TestUniqueNonPrimaryController.post({"unique": 1})
    TestUniqueNonPrimaryController.post({"unique": 2})
    with pytest.raises(Exception) as exception_info:
        TestUniqueNonPrimaryController.put({"unique": 2, "key": 1})
    assert {"": ["This document already exists."]} == exception_info.value.errors
    assert {"key": 1, "unique": 2} == exception_info.value.received_data


def test_post_id_is_valid(db):
    assert {"_id": "123456789abcdef012345678"} == TestIdController.post(
        {"_id": "123456789ABCDEF012345678"}
    )


def test_invalid_id_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestIdController.post({"_id": "invalid value"})
    assert {
        "_id": [
            "'invalid value' is not a valid ObjectId, it must be a 12-byte input or a 24-character hex string"
        ]
    } == exception_info.value.errors
    assert {"_id": "invalid value"} == exception_info.value.received_data


def test_get_all_with_none_primary_key_is_valid(db):
    assert {
        "non_unique_key": "2017-01-01",
        "unique_key": "test",
    } == TestIndexController.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    assert [
        {"non_unique_key": "2017-01-01", "unique_key": "test"}
    ] == TestIndexController.get({"unique_key": None})


def test_post_many_with_same_unique_index_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestIndexController.post_many(
            [
                {"unique_key": "test", "non_unique_key": "2017-01-01"},
                {"unique_key": "test", "non_unique_key": "2017-01-01"},
            ]
        )
    assert re.match(
        "{'writeErrors': [{'index': 1, 'code': 11000, 'errmsg': 'E11000 Duplicate Key Error', 'op': {'unique_key': 'test', 'non_unique_key': "
        "datetime.datetime(2017, 1, 1, 0, 0), '_id': ObjectId('.*')}}], 'nInserted': 1}".replace(
            "[", "\["
        )
        .replace("]", "\]")
        .replace("(", "\(")
        .replace(")", "\)"),
        str(exception_info.value.errors[""][0]),
    )
    assert [
        {"unique_key": "test", "non_unique_key": "2017-01-01"},
        {"unique_key": "test", "non_unique_key": "2017-01-01"},
    ] == exception_info.value.received_data


def test_post_without_primary_key_but_default_value_is_valid(db):
    assert {"key": "test", "optional": "test2"} == TestDefaultPrimaryKeyController.post(
        {"optional": "test2"}
    )


def test_get_on_default_value_is_valid(db):
    TestDefaultPrimaryKeyController.post({"optional": "test"})
    TestDefaultPrimaryKeyController.post({"key": "test2", "optional": "test2"})
    assert [{"key": "test", "optional": "test"}] == TestDefaultPrimaryKeyController.get(
        {"key": "test"}
    )


def test_delete_on_default_value_is_valid(db):
    TestDefaultPrimaryKeyController.post({"optional": "test"})
    TestDefaultPrimaryKeyController.post({"key": "test2", "optional": "test2"})
    assert 1 == TestDefaultPrimaryKeyController.delete({"key": "test"})


def test_put_without_primary_key_but_default_value_is_valid(db):
    assert {"key": "test", "optional": "test2"} == TestDefaultPrimaryKeyController.post(
        {"optional": "test2"}
    )
    assert (
        {"key": "test", "optional": "test2"},
        {"key": "test", "optional": "test3"},
    ) == TestDefaultPrimaryKeyController.put({"optional": "test3"})


def test_post_different_unique_index_is_valid(db):
    assert {
        "non_unique_key": "2017-01-01",
        "unique_key": "test",
    } == TestIndexController.post(
        {"unique_key": "test", "non_unique_key": "2017-01-01"}
    )
    assert {
        "non_unique_key": "2017-01-01",
        "unique_key": "test2",
    } == TestIndexController.post(
        {"unique_key": "test2", "non_unique_key": "2017-01-01"}
    )
    assert [
        {"non_unique_key": "2017-01-01", "unique_key": "test"},
        {"non_unique_key": "2017-01-01", "unique_key": "test2"},
    ] == TestIndexController.get({})


def test_post_many_with_wrong_type_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many([{"key": datetime.date(2007, 12, 5), "mandatory": 1}])
    assert {0: {"key": ["Not a valid str."]}} == exception_info.value.errors
    assert [
        {"key": datetime.date(2007, 12, 5), "mandatory": 1}
    ] == exception_info.value.received_data


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


def test_put_with_wrong_type_is_invalid(db):
    TestController.post({"key": "value1", "mandatory": 1})
    with pytest.raises(Exception) as exception_info:
        TestController.put({"key": "value1", "mandatory": "invalid value"})
    assert {"mandatory": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "key": "value1",
        "mandatory": "invalid value",
    } == exception_info.value.received_data


def test_put_with_optional_as_None_is_valid(db):
    TestController.post({"key": "value1", "mandatory": 1})
    TestController.put({"key": "value1", "mandatory": 1, "optional": None})
    assert [{"mandatory": 1, "key": "value1", "optional": None}] == TestController.get(
        {}
    )


def test_put_with_non_nullable_as_None_is_invalid(db):
    TestController.post({"key": "value1", "mandatory": 1})
    with pytest.raises(Exception) as exception_info:
        TestController.put({"key": "value1", "mandatory": None})
    assert {
        "mandatory": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "value1", "mandatory": None} == exception_info.value.received_data


def test_post_without_optional_is_valid(db):
    assert {"mandatory": 1, "key": "my_key", "optional": None} == TestController.post(
        {"key": "my_key", "mandatory": 1}
    )


def test_get_with_non_nullable_None_is_valid(db):
    assert {"mandatory": 1, "key": "my_key", "optional": None} == TestController.post(
        {"key": "my_key", "mandatory": 1}
    )
    assert [{"mandatory": 1, "key": "my_key", "optional": None}] == TestController.get(
        {"key": "my_key", "mandatory": None}
    )


def test_post_many_without_optional_is_valid(db):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": None},
        {"mandatory": 2, "key": "my_key2", "optional": None},
    ] == TestController.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )


def test_put_many_is_valid(db):
    TestController.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )
    assert (
        [
            {"mandatory": 1, "key": "my_key", "optional": None},
            {"mandatory": 2, "key": "my_key2", "optional": None},
        ],
        [
            {"mandatory": 1, "key": "my_key", "optional": "test"},
            {"mandatory": 3, "key": "my_key2", "optional": None},
        ],
    ) == TestController.put_many(
        [{"key": "my_key", "optional": "test"}, {"key": "my_key2", "mandatory": 3}]
    )


def test_post_with_optional_is_valid(db):
    assert {
        "mandatory": 1,
        "key": "my_key",
        "optional": "my_value",
    } == TestController.post({"key": "my_key", "mandatory": 1, "optional": "my_value"})


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


def test_post_list_of_str_is_sorted(db):
    assert {
        "key": "my_key",
        "list_field": ["a", "b", "c"],
    } == TestStringListController.post({"key": "my_key", "list_field": ["c", "a", "b"]})


def test_within_limits_is_valid(db):
    assert {
        "int_field": 100,
        "key": "111",
        "list_field": ["1", "2", "3"],
    } == TestLimitsController.post(
        {"key": "111", "list_field": ["1", "2", "3"], "int_field": 100}
    )


def test_outside_limits_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestLimitsController.post(
            {"key": "11", "list_field": ["1", "2", "3", "4", "5"], "int_field": 1000}
        )
    assert {
        "int_field": ['Value "1000" is too big. Maximum value is 999.'],
        "key": ['Value "11" is too small. Minimum length is 3.'],
        "list_field": [
            "['1', '2', '3', '4', '5'] contains too many values. Maximum length is 3."
        ],
    } == exception_info.value.errors
    assert {
        "int_field": 1000,
        "key": "11",
        "list_field": ["1", "2", "3", "4", "5"],
    } == exception_info.value.received_data


def test_post_optional_missing_list_of_dict_is_valid(db):
    assert {
        "bool_field": False,
        "key": "my_key",
        "list_field": None,
    } == TestListController.post({"key": "my_key", "bool_field": False})


def test_post_optional_list_of_dict_as_None_is_valid(db):
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


def test_put_list_of_str_is_sorted(db):
    TestStringListController.post({"key": "my_key", "list_field": ["a", "c", "b"]})
    assert (
        {"key": "my_key", "list_field": ["a", "b", "c"]},
        {"key": "my_key", "list_field": ["d", "e", "f"]},
    ) == TestStringListController.put({"key": "my_key", "list_field": ["f", "e", "d"]})


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


def test_post_dict_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )


def test_post_missing_optional_dict_is_valid(db):
    assert {
        "dict_col": {"first_key": None, "second_key": None},
        "key": "my_key",
    } == TestOptionalDictController.post({"key": "my_key"})


def test_post_optional_dict_as_None_is_valid(db):
    assert {
        "dict_col": {"first_key": None, "second_key": None},
        "key": "my_key",
    } == TestOptionalDictController.post({"key": "my_key", "dict_col": None})


def test_put_missing_optional_dict_is_valid(db):
    TestOptionalDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
    ) == TestOptionalDictController.put({"key": "my_key"})


def test_post_empty_optional_dict_is_valid(db):
    assert {"key": "my_key", "dict_col": {}} == TestOptionalDictController.post(
        {"key": "my_key", "dict_col": {}}
    )


def test_put_empty_optional_dict_is_valid(db):
    TestOptionalDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
        {"key": "my_key", "dict_col": {}},
    ) == TestOptionalDictController.put({"key": "my_key", "dict_col": {}})


def test_put_optional_dict_as_None_is_valid(db):
    TestOptionalDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
    ) == TestOptionalDictController.put({"key": "my_key", "dict_col": None})


def test_get_optional_dict_as_None_is_valid(db):
    TestOptionalDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert [
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    ] == TestOptionalDictController.get({"dict_col": None})


def test_delete_optional_dict_as_None_is_valid(db):
    TestOptionalDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == TestOptionalDictController.delete({"dict_col": None})


def test_get_with_dot_notation_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": EnumTest.Value1, "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == TestDictController.get({"dict_col.first_key": EnumTest.Value1})


def test_get_with_dot_notation_multi_level_is_valid(db):
    assert {
        "dict_field": {
            "first_key": {"inner_key1": "Value1", "inner_key2": 3},
            "second_key": 3,
        },
        "key": "my_key",
    } == TestDictInDictController.post(
        {
            "key": "my_key",
            "dict_field": {
                "first_key": {"inner_key1": EnumTest.Value1, "inner_key2": 3},
                "second_key": 3,
            },
        }
    )
    assert {
        "dict_field": {
            "first_key": {"inner_key1": "Value2", "inner_key2": 3},
            "second_key": 3,
        },
        "key": "my_key2",
    } == TestDictInDictController.post(
        {
            "key": "my_key2",
            "dict_field": {
                "first_key": {"inner_key1": EnumTest.Value2, "inner_key2": 3},
                "second_key": 3,
            },
        }
    )
    assert [
        {
            "dict_field": {
                "first_key": {"inner_key1": "Value1", "inner_key2": 3},
                "second_key": 3,
            },
            "key": "my_key",
        }
    ] == TestDictInDictController.get(
        {"dict_field.first_key.inner_key1": EnumTest.Value1}
    )


def test_get_with_dot_notation_as_list_is_valid(db):
    TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": EnumTest.Value1, "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == TestDictController.get({"dict_col.first_key": [EnumTest.Value1]})


def test_get_with_multiple_results_dot_notation_as_list_is_valid(db):
    TestDictController.post_many(
        [
            {
                "key": "my_key",
                "dict_col": {"first_key": EnumTest.Value1, "second_key": 3},
            },
            {
                "key": "my_key2",
                "dict_col": {"first_key": EnumTest.Value2, "second_key": 4},
            },
        ]
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
        {"dict_col": {"first_key": "Value2", "second_key": 4}, "key": "my_key2"},
    ] == TestDictController.get(
        {"dict_col.first_key": [EnumTest.Value1, EnumTest.Value2]}
    )


def test_update_with_dot_notation_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
        {"dict_col": {"first_key": "Value1", "second_key": 4}, "key": "my_key"},
    ) == TestDictController.put({"key": "my_key", "dict_col.second_key": 4})


def test_update_with_dot_notation_invalid_value_is_invalid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(Exception) as exception_info:
        TestDictController.put(
            {"key": "my_key", "dict_col.second_key": "invalid integer"}
        )
    assert {"dict_col.second_key": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "key": "my_key",
        "dict_col.second_key": "invalid integer",
    } == exception_info.value.received_data


def test_delete_with_dot_notation_invalid_value_is_invalid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(Exception) as exception_info:
        TestDictController.delete({"dict_col.second_key": "invalid integer"})
    assert {"dict_col.second_key": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "dict_col.second_key": "invalid integer"
    } == exception_info.value.received_data


def test_delete_with_dot_notation_valid_value_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == TestDictController.delete({"dict_col.second_key": 3})


def test_delete_with_dot_notation_enum_value_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == TestDictController.delete({"dict_col.first_key": EnumTest.Value1})


def test_post_with_dot_notation_invalid_value_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestDictController.post(
            {
                "key": "my_key",
                "dict_col.first_key": "Value1",
                "dict_col.second_key": "invalid integer",
            }
        )
    assert {"dict_col.second_key": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "key": "my_key",
        "dict_col.first_key": "Value1",
        "dict_col.second_key": "invalid integer",
    } == exception_info.value.received_data


def test_post_with_dot_notation_valid_value_is_valid(db):
    assert {
        "key": "my_key",
        "dict_col": {"first_key": "Value2", "second_key": 1},
    } == TestDictController.post(
        {"key": "my_key", "dict_col.first_key": "Value2", "dict_col.second_key": 1}
    )


def test_get_with_unmatching_dot_notation_is_empty(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert [] == TestDictController.get({"dict_col.first_key": "Value2"})


def test_get_with_unknown_dot_notation_returns_everything(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == TestDictController.get({"dict_col.unknown": "Value1"})


def test_delete_with_dot_notation_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == TestDictController.delete({"dict_col.first_key": "Value1"})
    assert [] == TestDictController.get({})


def test_delete_with_unmatching_dot_notation_is_empty(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 0 == TestDictController.delete({"dict_col.first_key": "Value2"})
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == TestDictController.get({})


def test_delete_with_unknown_dot_notation_deletes_everything(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == TestDictController.delete({"dict_col.unknown": "Value2"})
    assert [] == TestDictController.get({})


def test_put_without_primary_key_is_invalid(db):
    TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(Exception) as exception_info:
        TestDictController.put({"dict_col": {"first_key": "Value2", "second_key": 4}})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {
        "dict_col": {"first_key": "Value2", "second_key": 4}
    } == exception_info.value.received_data


def test_post_dict_with_dot_notation_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col.first_key": "Value1", "dict_col.second_key": 3}
    )


def test_put_dict_with_dot_notation_is_valid(db):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == TestDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
        {"dict_col": {"first_key": "Value2", "second_key": 3}, "key": "my_key"},
    ) == TestDictController.put(
        {"key": "my_key", "dict_col.first_key": EnumTest.Value2}
    )


def test_post_dict_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestDictController.post({"key": "my_key", "dict_col": {"first_key": "Value1"}})
    assert {
        "dict_col.second_key": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {
        "key": "my_key",
        "dict_col": {"first_key": "Value1"},
    } == exception_info.value.received_data


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


def test_post_with_unknown_field_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestStrictController.post(
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            }
        )
    assert {"unknown": ["Unknown field"]} == exception_info.value.errors
    assert {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
        "unknown": "my_value",
    } == exception_info.value.received_data


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


def test_post_with_enum_is_valid(db):
    assert {
        "optional_with_default": "Test value",
        "key": 1,
        "enum_field": "Value1",
    } == TestAutoIncrementController.post(
        {"key": "my_key", "enum_field": EnumTest.Value1}
    )


def test_post_with_invalid_enum_choice_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestAutoIncrementController.post(
            {"key": "my_key", "enum_field": "InvalidValue"}
        )
    assert {
        "enum_field": ["Value \"InvalidValue\" is not within ['Value1', 'Value2']."]
    } == exception_info.value.errors
    assert {"enum_field": "InvalidValue"} == exception_info.value.received_data


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
    d = datetime.datetime.strptime("2017-05-15", "%Y-%m-%d").date()
    assert [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ] == TestDateController.get({"date_str": d})


def test_post_invalid_date_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "this is not a date",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
    assert {"date_str": ["Not a valid date."]} == exception_info.value.errors
    assert {
        "key": "my_key1",
        "date_str": "this is not a date",
        "datetime_str": "2016-09-23T23:59:59",
    } == exception_info.value.received_data


def test_get_invalid_date_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestDateController.get({"date_str": "this is not a date"})
    assert {"date_str": ["Not a valid date."]} == exception_info.value.errors
    assert {"date_str": "this is not a date"} == exception_info.value.received_data


def test_delete_invalid_date_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestDateController.delete({"date_str": "this is not a date"})
    assert {"date_str": ["Not a valid date."]} == exception_info.value.errors
    assert {"date_str": "this is not a date"} == exception_info.value.received_data


def test_get_with_unknown_fields_is_valid(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert [
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    ] == TestDateController.get({"date_str": "2018-12-30", "unknown_field": "value"})


def test_put_with_unknown_fields_is_valid(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    assert (
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        },
        {
            "key": "my_key1",
            "date_str": "2018-12-31",
            "datetime_str": "2016-09-23T23:59:59",
        },
    ) == TestDateController.put(
        {"key": "my_key1", "date_str": "2018-12-31", "unknown_field": "value"}
    )
    assert [
        {
            "key": "my_key1",
            "date_str": "2018-12-31",
            "datetime_str": "2016-09-23T23:59:59",
        }
    ] == TestDateController.get({"date_str": "2018-12-31"})
    assert [] == TestDateController.get({"date_str": "2018-12-30"})


def test_put_unexisting_is_invalid(db):
    TestDateController.post(
        {
            "key": "my_key1",
            "date_str": "2018-12-30",
            "datetime_str": "2016-09-23T23:59:59",
        }
    )
    with pytest.raises(Exception) as exception_info:
        TestDateController.put({"key": "my_key2"})
    assert {"key": "my_key2"} == exception_info.value.requested_data


def test_post_invalid_datetime_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "2016-09-23",
                "datetime_str": "This is not a valid datetime",
            }
        )
    assert {"datetime_str": ["Not a valid datetime."]} == exception_info.value.errors
    assert {
        "key": "my_key1",
        "date_str": "2016-09-23",
        "datetime_str": "This is not a valid datetime",
    } == exception_info.value.received_data


def test_post_datetime_for_a_date_is_valid(db):
    assert {
        "key": "my_key1",
        "date_str": "2017-05-01",
        "datetime_str": "2017-05-30T01:05:45",
    } == TestDateController.post(
        {
            "key": "my_key1",
            "date_str": datetime.datetime.strptime(
                "2017-05-01T01:05:45", "%Y-%m-%dT%H:%M:%S"
            ),
            "datetime_str": "2017-05-30T01:05:45",
        }
    )


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
    dt = datetime.datetime.strptime("2016-09-23T23:59:59", "%Y-%m-%dT%H:%M:%S")
    assert [
        {
            "date_str": "2017-05-15",
            "datetime_str": "2016-09-23T23:59:59",
            "key": "my_key1",
        }
    ] == TestDateController.get({"datetime_str": dt})


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
        "offset": inputs.natural,
    } == parser_types(TestController.query_get_parser)


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


def test_query_get_parser_with_dict(db):
    assert {
        "dict_col.first_key": str,
        "dict_col.second_key": int,
        "key": str,
        "limit": inputs.positive,
        "offset": inputs.natural,
    } == parser_types(TestDictController.query_get_parser)
    assert {
        "dict_col.first_key": "append",
        "dict_col.second_key": "append",
        "key": "append",
        "limit": "store",
        "offset": "store",
    } == parser_actions(TestDictController.query_get_parser)


def test_query_delete_parser(db):
    assert {"key": str, "mandatory": int, "optional": str} == parser_types(
        TestController.query_delete_parser
    )


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


def test_query_rollback_parser(db):
    assert {
        "dict_field.first_key": str,
        "dict_field.second_key": int,
        "key": str,
        "revision": inputs.positive,
    } == parser_types(TestVersionedController.query_rollback_parser)
    assert {
        "dict_field.first_key": "append",
        "dict_field.second_key": "append",
        "key": "append",
        "revision": "store",
    } == parser_actions(TestVersionedController.query_rollback_parser)


def test_query_delete_parser_with_dict(db):
    assert {
        "dict_col.first_key": str,
        "dict_col.second_key": int,
        "key": str,
    } == parser_types(TestDictController.query_delete_parser)
    assert {
        "dict_col.first_key": "append",
        "dict_col.second_key": "append",
        "key": "append",
    } == parser_actions(TestDictController.query_delete_parser)


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


def test_get_response_model_with_limits(db):
    assert "TestLimitsModel" == TestLimitsController.get_response_model.name
    assert {
        "int_field": "Integer",
        "key": "String",
        "list_field": ("List", {"list_field_inner": "String"}),
    } == TestLimitsController.get_response_model.fields_flask_type
    assert {
        "int_field": None,
        "key": None,
        "list_field": (None, {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_description
    assert {
        "int_field": None,
        "key": None,
        "list_field": (None, {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_enum
    assert {
        "int_field": 100,
        "key": "XXX",
        "list_field": (["my", "test"], {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_example
    assert {
        "int_field": None,
        "key": None,
        "list_field": (None, {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_default
    assert {
        "int_field": False,
        "key": False,
        "list_field": (False, {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_required
    assert {
        "int_field": False,
        "key": False,
        "list_field": (False, {"list_field_inner": None}),
    } == TestLimitsController.get_response_model.fields_readonly


def test_get_response_model_with_date(db):
    assert "TestDateModel" == TestDateController.get_response_model.name
    assert {
        "date_str": "Date",
        "datetime_str": "DateTime",
        "key": "String",
    } == TestDateController.get_response_model.fields_flask_type
    assert {
        "date_str": None,
        "datetime_str": None,
        "key": None,
    } == TestDateController.get_response_model.fields_description
    assert {
        "date_str": None,
        "datetime_str": None,
        "key": None,
    } == TestDateController.get_response_model.fields_enum
    assert {
        "date_str": "2017-09-24",
        "datetime_str": "2017-09-24T15:36:09",
        "key": "sample key",
    } == TestDateController.get_response_model.fields_example
    assert {
        "date_str": None,
        "datetime_str": None,
        "key": None,
    } == TestDateController.get_response_model.fields_default
    assert {
        "date_str": False,
        "datetime_str": False,
        "key": False,
    } == TestDateController.get_response_model.fields_required
    assert {
        "date_str": False,
        "datetime_str": False,
        "key": False,
    } == TestDateController.get_response_model.fields_readonly


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
        "collection": "sample_table_name",
    } == TestController.get_model_description()


def test_get_model_description_response_model(db):
    assert (
        "TestModelDescription"
        == TestController.get_model_description_response_model.name
    )
    assert {
        "collection": "String",
        "key": "String",
        "mandatory": "String",
        "optional": "String",
    } == TestController.get_model_description_response_model.fields_flask_type
