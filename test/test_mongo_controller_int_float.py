import pytest
from layaberr import ValidationFailed

import layabase
import layabase.database_mongo
import layabase.testing


@pytest.fixture
def controller():
    class TestController(layabase.CRUDController):
        class TestIntAndFloatModel:
            __tablename__ = "int_and_float"

            int_value = layabase.database_mongo.Column(int)
            float_value = layabase.database_mongo.Column(float)

        model = TestIntAndFloatModel

    _db = layabase.load("mongomock", [TestController])
    yield TestController
    layabase.testing.reset(_db)


def test_post_int_str_in_int_column(controller):
    assert controller.post({"int_value": "15", "float_value": 1.0}) == {
        "int_value": 15,
        "float_value": 1.0,
    }


def test_put_int_str_in_int_column(controller):
    controller.post({"int_value": 15, "float_value": 1.0})
    assert controller.put({"int_value": "16", "float_value": 1.0}) == (
        {"int_value": 15, "float_value": 1.0},
        {"int_value": 16, "float_value": 1.0},
    )


def test_delete_int_str_in_int_column(controller):
    controller.post({"int_value": 15, "float_value": 1.0})
    assert controller.delete({"int_value": "15"}) == 1


def test_post_float_str_in_float_column(controller):
    assert controller.post({"int_value": 15, "float_value": "1.3"}) == {
        "int_value": 15,
        "float_value": 1.3,
    }


def test_put_float_str_in_float_column(controller):
    controller.post({"int_value": 15, "float_value": 1.3})
    assert controller.put({"int_value": 15, "float_value": "1.4"}) == (
        {"int_value": 15, "float_value": 1.3},
        {"int_value": 15, "float_value": 1.4},
    )


def test_delete_float_str_in_float_column(controller):
    controller.post({"int_value": 15, "float_value": 1.3})
    assert controller.delete({"float_value": "1.3"}) == 1


def test_post_with_non_int_str_in_int_column(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"int_value": "abc", "float_value": 1.0})
    assert exception_info.value.errors == {"int_value": ["Not a valid int."]}
    assert exception_info.value.received_data == {
        "int_value": "abc",
        "float_value": 1.0,
    }


def test_post_with_non_float_str_in_float_column(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post({"int_value": 1, "float_value": "abc"})
    assert exception_info.value.errors == {"float_value": ["Not a valid float."]}
    assert exception_info.value.received_data == {"float_value": "abc", "int_value": 1}


def test_get_with_non_int_str_in_int_column(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.get({"int_value": "abc", "float_value": 1.0})
    assert exception_info.value.errors == {"int_value": ["Not a valid int."]}
    assert exception_info.value.received_data == {
        "int_value": "abc",
        "float_value": 1.0,
    }


def test_get_with_non_float_str_in_float_column(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.get({"int_value": 1, "float_value": "abc"})
    assert exception_info.value.errors == {"float_value": ["Not a valid float."]}
    assert exception_info.value.received_data == {"float_value": "abc", "int_value": 1}


def test_put_with_non_int_str_in_int_column(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put({"int_value": "abc", "float_value": 1.0})
    assert exception_info.value.errors == {"int_value": ["Not a valid int."]}
    assert exception_info.value.received_data == {
        "int_value": "abc",
        "float_value": 1.0,
    }


def test_put_with_non_float_str_in_float_column(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.put({"int_value": 1, "float_value": "abc"})
    assert exception_info.value.errors == {"float_value": ["Not a valid float."]}
    assert exception_info.value.received_data == {"float_value": "abc", "int_value": 1}


def test_delete_with_non_int_str_in_int_column(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.delete({"int_value": "abc", "float_value": 1.0})
    assert exception_info.value.errors == {"int_value": ["Not a valid int."]}
    assert exception_info.value.received_data == {
        "int_value": "abc",
        "float_value": 1.0,
    }


def test_delete_with_non_float_str_in_float_column(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.delete({"int_value": 1, "float_value": "abc"})
    assert exception_info.value.errors == {"float_value": ["Not a valid float."]}
    assert exception_info.value.received_data == {"float_value": "abc", "int_value": 1}


def test_get_is_valid_with_int_str_in_int_column(controller):
    controller.post({"int_value": 123, "float_value": 1.0})
    assert controller.get_one({"int_value": "123"}) == {
        "int_value": 123,
        "float_value": 1.0,
    }


def test_get_is_valid_with_float_str_in_float_column(controller):
    controller.post({"int_value": 1, "float_value": 1.23})
    assert controller.get_one({"float_value": "1.23"}) == {
        "int_value": 1,
        "float_value": 1.23,
    }
