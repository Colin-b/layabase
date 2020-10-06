import pytest
import sqlalchemy

import layabase


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        test_field = sqlalchemy.Column(sqlalchemy.Enum("chose1", "chose2"))

    controller = layabase.CRUDController(TestTable)
    layabase.load("sqlite:///:memory:", [controller])
    return controller


def test_post_controller_with_valid_value_for_field(controller):
    assert controller.post({"key": "0", "test_field": "chose1"}) == {
        "key": "0",
        "test_field": "chose1",
    }


def test_post_controller_with_invalid_value_for_field(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"key": "0", "test_field": "chose_invalid"})
    assert exception_info.value.received_data == {
        "key": "0",
        "test_field": "chose_invalid",
    }
    assert exception_info.value.errors == {
        "test_field": [
            "Must be one of: chose1, chose2.",
            "Longer than maximum length 6.",
        ]
    }


def test_postmany_controller_with_valid_values_for_field(controller):
    assert controller.post_many(
        [{"key": "0", "test_field": "chose1"}, {"key": "1", "test_field": "chose2"}]
    ) == [{"key": "0", "test_field": "chose1"}, {"key": "1", "test_field": "chose2"}]


def test_postmany_controller_with_valid_value_and_invalid_value_for_field(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many(
            [
                {"key": "0", "test_field": "chose1"},
                {"key": "1", "test_field": "chose_invalid"},
            ]
        )
    assert exception_info.value.received_data == [
        {"key": "0", "test_field": "chose1"},
        {"key": "1", "test_field": "chose_invalid"},
    ]
    assert exception_info.value.errors == {
        1: {
            "test_field": [
                "Must be one of: chose1, chose2.",
                "Longer than maximum length 6.",
            ]
        }
    }


def test_put_controller_with_valid_value_for_field(controller):
    controller.post({"key": "0", "test_field": "chose1"})
    assert controller.put({"key": "0", "test_field": "chose2"}) == (
        {"key": "0", "test_field": "chose1"},
        {"key": "0", "test_field": "chose2"},
    )


def test_put_controller_with_invalid_value_for_field(controller):
    controller.post({"key": "0", "test_field": "chose1"})
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({"key": "0", "test_field": "chose_invalid"})
    assert exception_info.value.received_data == {
        "key": "0",
        "test_field": "chose_invalid",
    }
    assert exception_info.value.errors == {
        "test_field": [
            "Must be one of: chose1, chose2.",
            "Longer than maximum length 6.",
        ]
    }


def test_put_multiple_rows_controller_with_valid_value_for_field(controller):
    controller.post_many(
        [{"key": "0", "test_field": "chose1"}, {"key": "1", "test_field": "chose2"}]
    )
    assert controller.put_many(
        [{"key": "0", "test_field": "chose2"}, {"key": "1", "test_field": "chose1"}]
    ) == (
        [{"key": "0", "test_field": "chose1"}, {"key": "1", "test_field": "chose2"}],
        [{"key": "0", "test_field": "chose2"}, {"key": "1", "test_field": "chose1"}],
    )
