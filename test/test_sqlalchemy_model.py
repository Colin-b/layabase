import pytest
import sqlalchemy
from layaberr import ValidationFailed, ModelCouldNotBeFound


import layabase
import layabase.testing


@pytest.fixture
def model():
    class TestModel:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    controller = layabase.CRUDController(TestModel)
    _db = layabase.load("sqlite:///:memory:", [controller])
    model_class = controller._model
    yield model_class
    layabase.testing.reset(_db)


def test_get_all_without_data_returns_empty_list(model):
    assert model.get_all() == []


def test_get_without_data_returns_empty_dict(model):
    assert model.get() == {}


def test_add_with_nothing_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert exception_info.value.received_data == {}


def test_add_with_something_else_than_dictionary_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add("fail")
    assert {"_schema": ["Invalid input type."]} == exception_info.value.errors
    assert "fail" == exception_info.value.received_data


def test_add_all_with_something_else_than_list_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add_all("fail")
    assert {"_schema": ["Invalid input type."]} == exception_info.value.errors
    assert "fail" == exception_info.value.received_data


def test_add_all_with_something_else_than_list_of_dict_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add_all(["fail"])
    assert {0: {"_schema": ["Invalid input type."]}} == exception_info.value.errors
    assert ["fail"] == exception_info.value.received_data


def test_update_with_something_else_than_dictionary_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.update("fail")
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert "fail" == exception_info.value.received_data


def test_update_all_with_something_else_than_list_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.update_all("fail")
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert "f" == exception_info.value.received_data


def test_update_all_with_something_else_than_list_of_dict_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.update_all(["fail"])
    assert {"": ["Must be a dictionary."]} == exception_info.value.errors
    assert "fail" == exception_info.value.received_data


def test_primary_keys_are_returned(model):
    assert ["key", "mandatory"] == model.get_primary_keys()


def test_add_with_empty_dict_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert exception_info.value.received_data == {}


def test_update_with_nothing_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.update(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert exception_info.value.received_data == {}


def test_update_with_empty_dict_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.update({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert exception_info.value.received_data == {}
    assert model.get() == {}


def test_remove_without_nothing_do_not_fail(model):
    assert 0 == model.remove()
    assert model.get() == {}


def test_add_without_mandatory_field_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add({"key": "my_key"})
    assert {
        "mandatory": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "my_key"} == exception_info.value.received_data
    assert model.get() == {}


def test_add_without_key_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add({"mandatory": 1})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {"mandatory": 1} == exception_info.value.received_data
    assert model.get() == {}


def test_add_with_wrong_type_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add({"key": 256, "mandatory": 1})
    assert {"key": ["Not a valid string."]} == exception_info.value.errors
    assert {"key": 256, "mandatory": 1} == exception_info.value.received_data
    assert model.get() == {}


def test_update_with_wrong_type_is_invalid(model):
    model.add({"key": "value1", "mandatory": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        model.update({"key": "value1", "mandatory": "invalid_value"})
    assert {"mandatory": ["Not a valid integer."]} == exception_info.value.errors
    assert {
        "key": "value1",
        "mandatory": "invalid_value",
    } == exception_info.value.received_data


def test_add_all_with_nothing_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add_all(None)
    assert {"": ["No data provided."]} == exception_info.value.errors


def test_add_all_with_empty_dict_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add_all({})
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}


def test_add_all_without_mandatory_field_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add_all(
            [
                {"key": "my_key"},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ]
        )
    assert exception_info.value.errors == {
        0: {"mandatory": ["Missing data for required field."]}
    }
    assert exception_info.value.received_data == [
        {"key": "my_key"},
        {"key": "my_key", "mandatory": 1, "optional": "my_value"},
    ]
    assert model.get() == {}


def test_add_all_without_key_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add_all(
            [
                {"mandatory": 1},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ]
        )
    assert {
        0: {"key": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [
        {"mandatory": 1},
        {"key": "my_key", "mandatory": 1, "optional": "my_value"},
    ] == exception_info.value.received_data
    assert model.get() == {}


def test_add_all_with_wrong_type_is_invalid(model):
    with pytest.raises(ValidationFailed) as exception_info:
        model.add_all(
            [
                {"key": 256, "mandatory": 1},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ]
        )
    assert {0: {"key": ["Not a valid string."]}} == exception_info.value.errors
    assert [
        {"key": 256, "mandatory": 1},
        {"key": "my_key", "mandatory": 1, "optional": "my_value"},
    ] == exception_info.value.received_data
    assert model.get() == {}


def test_add_without_optional_is_valid(model):
    assert {"mandatory": 1, "key": "my_key", "optional": None} == model.add(
        {"key": "my_key", "mandatory": 1}
    )
    assert {"key": "my_key", "mandatory": 1, "optional": None} == model.get()


def test_add_with_optional_is_valid(model):
    assert {"mandatory": 1, "key": "my_key", "optional": "my_value"} == model.add(
        {"key": "my_key", "mandatory": 1, "optional": "my_value"}
    )
    assert model.get() == {"key": "my_key", "mandatory": 1, "optional": "my_value"}


def test_update_unexisting_is_invalid(model):
    with pytest.raises(ModelCouldNotBeFound) as exception_info:
        model.update({"key": "my_key", "mandatory": 1, "optional": "my_value"})
    assert {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
    } == exception_info.value.requested_data


def test_add_with_unknown_field_is_valid(model):
    assert {"mandatory": 1, "key": "my_key", "optional": "my_value"} == model.add(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    )
    assert model.get() == {"key": "my_key", "mandatory": 1, "optional": "my_value"}


def test_get_without_filter_is_retrieving_the_only_item(model):
    model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert {"mandatory": 1, "optional": "my_value1", "key": "my_key1"} == model.get()


def test_get_without_filter_is_failing_if_more_than_one_item_exists(model):
    model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    with pytest.raises(ValidationFailed) as exception_info:
        model.get()
    assert {
        "": ["More than one result: Consider another filtering."]
    } == exception_info.value.errors
    assert exception_info.value.received_data == {}


def test_get_all_without_filter_is_retrieving_everything_after_multiple_posts(model):
    model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert model.get_all() == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]


def test_get_all_without_filter_is_retrieving_everything(model):
    model.add_all(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert model.get_all() == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]


def test_get_all_with_filter_is_retrieving_subset_after_multiple_posts(model):
    model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert model.get_all(optional="my_value1") == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ]


def test_get_all_with_filter_is_retrieving_subset(model):
    model.add_all(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert model.get_all(optional="my_value1") == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ]


def test_get_all_order_by(model):
    model.add_all(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 1, "optional": "my_value2"},
            {"key": "my_key3", "mandatory": -1, "optional": "my_value3"},
        ]
    )
    assert model.get_all(
        order_by=[sqlalchemy.asc(model.mandatory), sqlalchemy.desc(model.key)]
    ) == [
        {"key": "my_key3", "mandatory": -1, "optional": "my_value3"},
        {"key": "my_key2", "mandatory": 1, "optional": "my_value2"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
    ]


def test_get_with_filter_is_retrieving_the_proper_row_after_multiple_posts(model):
    model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert model.get(optional="my_value1") == {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value1",
    }


def test_get_with_filter_is_retrieving_the_proper_row(model):
    model.add_all(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert model.get(optional="my_value1") == {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value1",
    }


def test_update_is_updating(model):
    model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert model.update({"key": "my_key1", "optional": "my_value"}) == (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    )
    assert model.get(mandatory=1) == {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value",
    }


def test_update_is_updating_and_previous_value_cannot_be_used_to_filter(model):
    model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    model.update({"key": "my_key1", "optional": "my_value"})
    assert model.get(optional="my_value1") == {}


def test_remove_with_filter_is_removing_the_proper_row(model):
    model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert model.remove(key="my_key1") == 1
    assert model.get_all() == [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ]


def test_remove_without_filter_is_removing_everything(model):
    model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert model.remove() == 2
    assert model.get_all() == []
