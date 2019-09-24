import enum

import pytest

from layabase import database, database_mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestOptionalDictController(database.CRUDController):
    pass


def _create_models(base):
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

    TestOptionalDictController.model(TestOptionalDictModel)

    return [TestOptionalDictModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
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


def test_post_missing_optional_dict_is_valid(db):
    assert {
        "dict_col": {"first_key": None, "second_key": None},
        "key": "my_key",
    } == TestOptionalDictController.post({"key": "my_key"})


def test_post_optional_dict_as_none_is_valid(db):
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


def test_put_optional_dict_as_none_is_valid(db):
    TestOptionalDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert (
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
    ) == TestOptionalDictController.put({"key": "my_key", "dict_col": None})


def test_get_optional_dict_as_none_is_valid(db):
    TestOptionalDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert [
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    ] == TestOptionalDictController.get({"dict_col": None})


def test_delete_optional_dict_as_none_is_valid(db):
    TestOptionalDictController.post(
        {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
    )
    assert 1 == TestOptionalDictController.delete({"dict_col": None})
