import pytest

import layabase.mongo


def test_str_column_cannot_auto_increment():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.Column(should_auto_increment=True)
    assert str(exception_info.value) == "Only int fields can be auto incremented."


def test_column_str():
    my_column = layabase.mongo.Column()
    assert str(my_column) == "my_column"


def test_auto_incremented_field_cannot_be_non_nullable():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.Column(int, should_auto_increment=True, is_nullable=False)
    assert (
        str(exception_info.value)
        == "A field cannot be mandatory and auto incremented at the same time."
    )


def test_field_with_default_value_cannot_be_non_nullable():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.Column(default_value="test", is_nullable=False)
    assert (
        str(exception_info.value)
        == "A field cannot be mandatory and having a default value at the same time."
    )


def test_fields_should_be_provided_in_dict_column():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.DictColumn(fields=None, is_nullable=False)
    assert str(exception_info.value) == "fields or get_fields must be provided."


def test_int_column_with_min_value_not_int_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.Column(int, min_value="test", max_value=999)
    assert str(exception_info.value) == "Minimum value should be of <class 'int'> type."


def test_int_column_with_max_value_not_int_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.Column(int, min_value=100, max_value="test")
    assert str(exception_info.value) == "Maximum value should be of <class 'int'> type."


def test_int_column_with_max_value_smaller_than_min_value_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.Column(int, min_value=100, max_value=50)
    assert (
        str(exception_info.value)
        == "Maximum value should be superior or equals to minimum value"
    )


def test_int_column_with_negative_min_length_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.Column(int, min_length=-100, max_value=50)
    assert str(exception_info.value) == "Minimum length should be positive"


def test_int_column_with_negative_max_length_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.Column(int, min_length=100, max_length=-100)
    assert str(exception_info.value) == "Maximum length should be positive"


def test_column_with_index_type_and_is_primary_key_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.Column(
            int, index_type=layabase.mongo.IndexType.Unique, is_primary_key=True
        )
    assert (
        str(exception_info.value)
        == "Primary key fields are supposed to be indexed as unique."
    )


def test_int_column_with_max_length_smaller_than_min_length_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.Column(int, min_length=100, max_length=50)
    assert (
        str(exception_info.value)
        == "Maximum length should be superior or equals to minimum length"
    )


def test_int_column_with_not_int_example_is_invalid():
    with pytest.raises(Exception) as exception_info:
        layabase.mongo.Column(int, example="test", counter=100, choices=[1, 2])
    assert str(exception_info.value) == "Example must be of field type."
