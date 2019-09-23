import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo


class TestIntAndFloatController(database.CRUDController):
    pass


def _create_models(base):
    class TestIntAndFloatModel(
        database_mongo.CRUDModel, base=base, table_name="int_and_float"
    ):
        int_value = database_mongo.Column(int)
        float_value = database_mongo.Column(float)

    TestIntAndFloatController.model(TestIntAndFloatModel)

    return [TestIntAndFloatModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    yield _db
    database.reset(_db)


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
    with pytest.raises(ValidationFailed) as exception_info:
        TestIntAndFloatController.post({"int_value": "abc", "float_value": 1.0})
    assert {"int_value": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "int_value": "abc",
        "float_value": 1.0,
    } == exception_info.value.received_data


def test_post_with_non_float_str_in_float_column(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestIntAndFloatController.post({"int_value": 1, "float_value": "abc"})
    assert {"float_value": ["Not a valid float."]} == exception_info.value.errors
    assert {"float_value": "abc", "int_value": 1} == exception_info.value.received_data


def test_get_with_non_int_str_in_int_column(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestIntAndFloatController.get({"int_value": "abc", "float_value": 1.0})
    assert {"int_value": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "int_value": "abc",
        "float_value": 1.0,
    } == exception_info.value.received_data


def test_get_with_non_float_str_in_float_column(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestIntAndFloatController.get({"int_value": 1, "float_value": "abc"})
    assert {"float_value": ["Not a valid float."]} == exception_info.value.errors
    assert {"float_value": "abc", "int_value": 1} == exception_info.value.received_data


def test_put_with_non_int_str_in_int_column(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestIntAndFloatController.put({"int_value": "abc", "float_value": 1.0})
    assert {"int_value": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "int_value": "abc",
        "float_value": 1.0,
    } == exception_info.value.received_data


def test_put_with_non_float_str_in_float_column(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestIntAndFloatController.put({"int_value": 1, "float_value": "abc"})
    assert {"float_value": ["Not a valid float."]} == exception_info.value.errors
    assert {"float_value": "abc", "int_value": 1} == exception_info.value.received_data


def test_delete_with_non_int_str_in_int_column(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestIntAndFloatController.delete({"int_value": "abc", "float_value": 1.0})
    assert {"int_value": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "int_value": "abc",
        "float_value": 1.0,
    } == exception_info.value.received_data


def test_delete_with_non_float_str_in_float_column(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestIntAndFloatController.delete({"int_value": 1, "float_value": "abc"})
    assert {"float_value": ["Not a valid float."]} == exception_info.value.errors
    assert {"float_value": "abc", "int_value": 1} == exception_info.value.received_data


def test_get_is_valid_with_int_str_in_int_column(db):
    TestIntAndFloatController.post({"int_value": 123, "float_value": 1.0})
    assert {"int_value": 123, "float_value": 1.0} == TestIntAndFloatController.get_one(
        {"int_value": "123"}
    )


def test_get_is_valid_with_float_str_in_float_column(db):
    TestIntAndFloatController.post({"int_value": 1, "float_value": 1.23})
    assert {"int_value": 1, "float_value": 1.23} == TestIntAndFloatController.get_one(
        {"float_value": "1.23"}
    )
