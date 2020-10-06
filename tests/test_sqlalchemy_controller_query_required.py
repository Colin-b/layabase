import pytest
import sqlalchemy

import layabase


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(
            sqlalchemy.Integer,
            nullable=False,
            info={"layabase": {"required_on_query": True}},
        )

    controller = layabase.CRUDController(TestTable)
    layabase.load("sqlite:///:memory:", [controller])
    return controller


def test_get_without_required_field(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.get({})
    assert exception_info.value.received_data == {}
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }


def test_get_with_required_field(controller):
    assert controller.get({"mandatory": 1}) == []


def test_get_one_without_required_field(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.get_one({})
    assert exception_info.value.received_data == {}
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }


def test_get_one_with_required_field(controller):
    assert controller.get_one({"mandatory": 1}) == {}


def test_delete_without_required_field(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.delete({})
    assert exception_info.value.received_data == {}
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }


def test_delete_with_required_field(controller):
    assert controller.delete({"mandatory": 1}) == 0
