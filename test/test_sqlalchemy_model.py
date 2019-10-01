import pytest
import sqlalchemy
from layaberr import ValidationFailed, ModelCouldNotBeFound


import layabase
import layabase.testing


@pytest.fixture
def controller():
    class TestModel:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    controller = layabase.CRUDController(TestModel)
    _db = layabase.load("sqlite:///:memory:", [controller])
    yield controller
    layabase.testing.reset(_db)


def test_primary_keys_are_returned(controller: layabase.CRUDController):
    inserted = controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert (
        controller.get_url("/test", *inserted)
        == "/test?key=my_key1&mandatory=1&key=my_key2&mandatory=2"
    )


def test_remove_without_nothing_do_not_fail(controller: layabase.CRUDController):
    assert controller.delete({}) == 0
    assert controller.get_one({}) == {}


def test_add_without_mandatory_field_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"key": "my_key"})
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }
    assert exception_info.value.received_data == {"key": "my_key"}
    assert controller.get_one({}) == {}


def test_add_without_key_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"mandatory": 1})
    assert exception_info.value.errors == {"key": ["Missing data for required field."]}
    assert exception_info.value.received_data == {"mandatory": 1}
    assert controller.get_one({}) == {}


def test_add_with_wrong_type_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"key": 256, "mandatory": 1})
    assert exception_info.value.errors == {"key": ["Not a valid string."]}
    assert exception_info.value.received_data == {"key": 256, "mandatory": 1}
    assert controller.get_one({}) == {}


def test_update_with_wrong_type_is_invalid(controller: layabase.CRUDController):
    controller.post({"key": "value1", "mandatory": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put({"key": "value1", "mandatory": "invalid_value"})
    assert exception_info.value.errors == {"mandatory": ["Not a valid integer."]}
    assert exception_info.value.received_data == {
        "key": "value1",
        "mandatory": "invalid_value",
    }


def test_add_all_without_mandatory_field_is_invalid(
    controller: layabase.CRUDController,
):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many(
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
    assert controller.get_one({}) == {}


def test_add_all_without_key_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many(
            [
                {"mandatory": 1},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ]
        )
    assert exception_info.value.errors == {
        0: {"key": ["Missing data for required field."]}
    }
    assert exception_info.value.received_data == [
        {"mandatory": 1},
        {"key": "my_key", "mandatory": 1, "optional": "my_value"},
    ]
    assert controller.get_one({}) == {}


def test_add_all_with_wrong_type_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post_many(
            [
                {"key": 256, "mandatory": 1},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ]
        )
    assert exception_info.value.errors == {0: {"key": ["Not a valid string."]}}
    assert exception_info.value.received_data == [
        {"key": 256, "mandatory": 1},
        {"key": "my_key", "mandatory": 1, "optional": "my_value"},
    ]
    assert controller.get_one({}) == {}


def test_add_without_optional_is_valid(controller: layabase.CRUDController):
    assert controller.post({"key": "my_key", "mandatory": 1}) == {
        "mandatory": 1,
        "key": "my_key",
        "optional": None,
    }
    assert controller.get_one({}) == {"key": "my_key", "mandatory": 1, "optional": None}


def test_add_with_optional_is_valid(controller: layabase.CRUDController):
    assert controller.post(
        {"key": "my_key", "mandatory": 1, "optional": "my_value"}
    ) == {"mandatory": 1, "key": "my_key", "optional": "my_value"}
    assert controller.get_one({}) == {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
    }


def test_update_unexisting_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(ModelCouldNotBeFound) as exception_info:
        controller.put({"key": "my_key", "mandatory": 1, "optional": "my_value"})
    assert exception_info.value.requested_data == {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
    }


def test_add_with_unknown_field_is_valid(controller: layabase.CRUDController):
    assert controller.post(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    ) == {"mandatory": 1, "key": "my_key", "optional": "my_value"}
    assert controller.get_one({}) == {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
    }


def test_get_without_filter_is_retrieving_the_only_item(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller.get_one({}) == {
        "mandatory": 1,
        "optional": "my_value1",
        "key": "my_key1",
    }


def test_get_without_filter_is_failing_if_more_than_one_item_exists(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    with pytest.raises(ValidationFailed) as exception_info:
        controller.get_one({})
    assert exception_info.value.errors == {
        "": ["More than one result: Consider another filtering."]
    }
    assert exception_info.value.received_data == {}


def test_get_all_without_filter_is_retrieving_everything_after_multiple_posts(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]


def test_get_all_without_filter_is_retrieving_everything(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert controller.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]


def test_get_all_with_filter_is_retrieving_subset_after_multiple_posts(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({"optional": "my_value1"}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ]


def test_get_all_with_filter_is_retrieving_subset(controller: layabase.CRUDController):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert controller.get({"optional": "my_value1"}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ]


def test_get_all_order_by(controller: layabase.CRUDController):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 1, "optional": "my_value2"},
            {"key": "my_key3", "mandatory": -1, "optional": "my_value3"},
        ]
    )
    assert controller.get(
        {
            "order_by": [
                sqlalchemy.asc(controller._model.mandatory),
                sqlalchemy.desc(controller._model.key),
            ]
        }
    ) == [
        {"key": "my_key3", "mandatory": -1, "optional": "my_value3"},
        {"key": "my_key2", "mandatory": 1, "optional": "my_value2"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
    ]


def test_get_with_filter_is_retrieving_the_proper_row_after_multiple_posts(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get_one({"optional": "my_value1"}) == {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value1",
    }


def test_get_with_filter_is_retrieving_the_proper_row(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert controller.get_one({"optional": "my_value1"}) == {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value1",
    }


def test_update_is_updating(controller: layabase.CRUDController):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller.put({"key": "my_key1", "optional": "my_value"}) == (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    )
    assert controller.get_one({"mandatory": 1}) == {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value",
    }


def test_update_is_updating_and_previous_value_cannot_be_used_to_filter(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "optional": "my_value"})
    assert controller.get_one({"optional": "my_value1"}) == {}


def test_remove_with_filter_is_removing_the_proper_row(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.delete({"key": "my_key1"}) == 1
    assert controller.get({}) == [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ]


def test_remove_without_filter_is_removing_everything(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.delete({}) == 2
    assert controller.get({}) == []
