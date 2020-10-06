import enum

import pytest

import layabase
import layabase.mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        dict_col = layabase.mongo.DictColumn(
            get_fields=lambda document: {
                "first_key": layabase.mongo.Column(EnumTest, is_nullable=False),
                "second_key": layabase.mongo.Column(int, is_nullable=False),
            }
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_post_dict_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )


def test_get_with_dot_notation_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": EnumTest.Value1, "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == controller.get({"dict_col.first_key": EnumTest.Value1})


def test_get_with_dot_notation_as_list_is_valid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": EnumTest.Value1, "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == controller.get({"dict_col.first_key": [EnumTest.Value1]})


def test_get_with_multiple_results_dot_notation_as_list_is_valid(controller):
    controller.post_many(
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
    ] == controller.get({"dict_col.first_key": [EnumTest.Value1, EnumTest.Value2]})


def test_update_with_dot_notation_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
        {"dict_col": {"first_key": "Value1", "second_key": 4}, "key": "my_key"},
    ) == controller.put({"key": "my_key", "dict_col.second_key": 4})


def test_update_with_dot_notation_invalid_value_is_invalid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({"key": "my_key", "dict_col.second_key": "invalid integer"})
    assert {"dict_col.second_key": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "key": "my_key",
        "dict_col.second_key": "invalid integer",
    } == exception_info.value.received_data


def test_delete_with_dot_notation_invalid_value_is_invalid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.delete({"dict_col.second_key": "invalid integer"})
    assert {"dict_col.second_key": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "dict_col.second_key": "invalid integer"
    } == exception_info.value.received_data


def test_delete_with_dot_notation_valid_value_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == controller.delete({"dict_col.second_key": 3})


def test_delete_with_dot_notation_enum_value_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == controller.delete({"dict_col.first_key": EnumTest.Value1})


def test_post_with_dot_notation_invalid_value_is_invalid(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post(
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


def test_post_with_dot_notation_valid_value_is_valid(controller):
    assert {
        "key": "my_key",
        "dict_col": {"first_key": "Value2", "second_key": 1},
    } == controller.post(
        {"key": "my_key", "dict_col.first_key": "Value2", "dict_col.second_key": 1}
    )


def test_get_with_unmatching_dot_notation_is_empty(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert [] == controller.get({"dict_col.first_key": "Value2"})


def test_get_with_unknown_dot_notation_returns_everything(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == controller.get({"dict_col.unknown": "Value1"})


def test_delete_with_dot_notation_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == controller.delete({"dict_col.first_key": "Value1"})
    assert [] == controller.get({})


def test_delete_with_unmatching_dot_notation_is_empty(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 0 == controller.delete({"dict_col.first_key": "Value2"})
    assert [
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}
    ] == controller.get({})


def test_delete_with_unknown_dot_notation_deletes_everything(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == controller.delete({"dict_col.unknown": "Value2"})
    assert [] == controller.get({})


def test_put_without_primary_key_is_invalid(controller):
    controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({"dict_col": {"first_key": "Value2", "second_key": 4}})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {
        "dict_col": {"first_key": "Value2", "second_key": 4}
    } == exception_info.value.received_data


def test_post_dict_with_dot_notation_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col.first_key": "Value1", "dict_col.second_key": 3}
    )


def test_put_dict_with_dot_notation_is_valid(controller):
    assert {
        "dict_col": {"first_key": "Value1", "second_key": 3},
        "key": "my_key",
    } == controller.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
        {"dict_col": {"first_key": "Value2", "second_key": 3}, "key": "my_key"},
    ) == controller.put({"key": "my_key", "dict_col.first_key": EnumTest.Value2})


def test_post_dict_is_invalid(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"key": "my_key", "dict_col": {"first_key": "Value1"}})
    assert {
        "dict_col.second_key": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {
        "key": "my_key",
        "dict_col": {"first_key": "Value1"},
    } == exception_info.value.received_data
