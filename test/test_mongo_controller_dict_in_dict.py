import enum

import pytest

from layabase import database, database_mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestDictInDictController(database.CRUDController):
    pass


def _create_models(base):
    class TestDictInDictModel(
        database_mongo.CRUDModel, base=base, table_name="dict_in_dict_table_name"
    ):
        key = database_mongo.Column(is_primary_key=True)
        dict_field = database_mongo.DictColumn(
            fields={
                "first_key": database_mongo.DictColumn(
                    fields={
                        "inner_key1": database_mongo.Column(
                            EnumTest, is_nullable=False
                        ),
                        "inner_key2": database_mongo.Column(int, is_nullable=False),
                    },
                    is_required=True,
                ),
                "second_key": database_mongo.Column(int, is_nullable=False),
            },
            is_required=True,
        )

    TestDictInDictController.model(TestDictInDictModel)

    return [TestDictInDictModel]


@pytest.fixture
def db():
    _db = database.load("mongomock", _create_models)
    yield _db
    database.reset(_db)


def test_get_with_dot_notation_multi_level_is_valid(db):
    assert {
        "dict_field": {
            "first_key": {"inner_key1": "Value1", "inner_key2": 3},
            "second_key": 3,
        },
        "key": "my_key",
    } == TestDictInDictController.post(
        {
            "key": "my_key",
            "dict_field": {
                "first_key": {"inner_key1": EnumTest.Value1, "inner_key2": 3},
                "second_key": 3,
            },
        }
    )
    assert {
        "dict_field": {
            "first_key": {"inner_key1": "Value2", "inner_key2": 3},
            "second_key": 3,
        },
        "key": "my_key2",
    } == TestDictInDictController.post(
        {
            "key": "my_key2",
            "dict_field": {
                "first_key": {"inner_key1": EnumTest.Value2, "inner_key2": 3},
                "second_key": 3,
            },
        }
    )
    assert [
        {
            "dict_field": {
                "first_key": {"inner_key1": "Value1", "inner_key2": 3},
                "second_key": 3,
            },
            "key": "my_key",
        }
    ] == TestDictInDictController.get(
        {"dict_field.first_key.inner_key1": EnumTest.Value1}
    )
