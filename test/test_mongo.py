import pymongo
import pytest

import layabase
import layabase.database_mongo
import layabase.testing


def test_str_column_cannot_auto_increment():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(should_auto_increment=True)
    assert str(exception_info.value) == "Only int fields can be auto incremented."


def test_auto_incremented_field_cannot_be_non_nullable():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(
            int, should_auto_increment=True, is_nullable=False
        )
    assert (
        str(exception_info.value)
        == "A field cannot be mandatory and auto incremented at the same time."
    )


def test_field_with_default_value_cannot_be_non_nullable():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(default_value="test", is_nullable=False)
    assert (
        str(exception_info.value)
        == "A field cannot be mandatory and having a default value at the same time."
    )


def test_spaces_are_forbidden_at_start_of_column_name():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(name="   test")
    assert (
        str(exception_info.value)
        == "   test is not a valid name. Spaces are not allowed at start or end of field names."
    )


def test_spaces_are_forbidden_at_end_of_column_name():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(name="test   ")
    assert (
        str(exception_info.value)
        == "test    is not a valid name. Spaces are not allowed at start or end of field names."
    )


def test_spaces_are_forbidden_at_start_and_end_of_column_name():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(name="   test   ")
    assert (
        str(exception_info.value)
        == "   test    is not a valid name. Spaces are not allowed at start or end of field names."
    )


def test_none_connection_string_is_invalid():
    with pytest.raises(layabase.NoDatabaseProvided) as exception_info:
        layabase.load(None, None)
    assert str(exception_info.value) == "A database connection URL must be provided."


def test_empty_connection_string_is_invalid():
    with pytest.raises(layabase.NoDatabaseProvided) as exception_info:
        layabase.load("", None)
    assert str(exception_info.value) == "A database connection URL must be provided."


def test_no_create_models_function_is_invalid():
    with pytest.raises(layabase.NoRelatedControllers) as exception_info:
        layabase.load("mongomock", None)
    assert str(exception_info.value) == "A list of CRUDController must be provided."


def test_dots_are_forbidden_in_column_name():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(name="te.st")
    assert (
        str(exception_info.value)
        == "te.st is not a valid name. Dots are not allowed in Mongo field names."
    )


def test_fields_should_be_provided_in_dict_column():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.DictColumn(fields=None, is_nullable=False)
    assert str(exception_info.value) == "fields or get_fields must be provided."


def test_int_column_with_min_value_not_int_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(int, min_value="test", max_value=999)
    assert str(exception_info.value) == "Minimum value should be of <class 'int'> type."


def test_int_column_with_max_value_not_int_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(int, min_value=100, max_value="test")
    assert str(exception_info.value) == "Maximum value should be of <class 'int'> type."


def test_int_column_with_max_value_smaller_than_min_value_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(int, min_value=100, max_value=50)
    assert (
        str(exception_info.value)
        == "Maximum value should be superior or equals to minimum value"
    )


def test_int_column_with_negative_min_length_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(int, min_length=-100, max_value=50)
    assert str(exception_info.value) == "Minimum length should be positive"


def test_int_column_with_negative_max_length_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(int, min_length=100, max_length=-100)
    assert str(exception_info.value) == "Maximum length should be positive"


def test_column_with_index_type_and_is_primary_key_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(
            int,
            index_type=layabase.database_mongo.IndexType.Unique,
            is_primary_key=True,
        )
    assert (
        str(exception_info.value)
        == "Primary key fields are supposed to be indexed as unique."
    )


def test_int_column_with_max_length_smaller_than_min_length_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(int, min_length=100, max_length=50)
    assert (
        str(exception_info.value)
        == "Maximum length should be superior or equals to minimum length"
    )


def test_int_column_with_not_int_example_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.database_mongo.Column(int, example="test", counter=100, choices=[1, 2])
    assert str(exception_info.value) == "Example must be of field type."


def test_2entities_on_same_collection_without_pk():
    class TestEntitySameCollection1Controller(layabase.CRUDController):
        class TestEntitySameCollection1:
            __tablename__ = "sample_table_name_2entities"

            key = layabase.database_mongo.Column(str, is_primary_key=True)
            mandatory = layabase.database_mongo.Column(int, is_nullable=False)
            optional = layabase.database_mongo.Column(str)

        model = TestEntitySameCollection1
        history = True

    mongo_base = layabase.load("mongomock", [TestEntitySameCollection1Controller])
    TestEntitySameCollection1Controller.post({"key": "1", "mandatory": 2})
    TestEntitySameCollection1Controller.post({"key": "2", "mandatory": 2})

    class TestEntitySameCollection2Controller(layabase.CRUDController):
        class TestEntitySameCollection2:
            __tablename__ = "sample_table_name_2entities"

        model = TestEntitySameCollection2
        history = True

    # This call is performed using the internal function because we want to simulate an already filled database
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        layabase.database_mongo._create_model(
            TestEntitySameCollection2Controller, mongo_base
        )


def test_2entities_on_same_collection_with_pk():
    class TestEntitySameCollection1Controller(layabase.CRUDController):
        class TestEntitySameCollection1:
            __tablename__ = "sample_table_name_2entities"

            key = layabase.database_mongo.Column(str, is_primary_key=True)
            mandatory = layabase.database_mongo.Column(int, is_nullable=False)
            optional = layabase.database_mongo.Column(str)

        model = TestEntitySameCollection1
        history = True

    layabase.load("mongomock", [TestEntitySameCollection1Controller])
    TestEntitySameCollection1Controller.post({"key": "1", "mandatory": 2})
    TestEntitySameCollection1Controller.post({"key": "2", "mandatory": 2})

    class TestEntitySameCollection2Controller(layabase.CRUDController):
        class TestEntitySameCollection2:
            __tablename__ = "sample_table_name_2entities"

        model = TestEntitySameCollection2
        history = True
        skip_update_indexes = True

    layabase.load("mongomock", [TestEntitySameCollection2Controller])
    TestEntitySameCollection1Controller.post({"key": "3", "mandatory": 2})
    assert len(TestEntitySameCollection1Controller.get({})) == 3
