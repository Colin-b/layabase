import enum

import pytest

import layabase
import layabase.database_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller():
    class TestController(layabase.CRUDController):
        class TestDictInDictModel:
            __tablename__ = "dict_in_dict_table_name"

            key = layabase.database_mongo.Column(is_primary_key=True)
            dict_field = layabase.database_mongo.DictColumn(
                fields={
                    "first_key": layabase.database_mongo.DictColumn(
                        fields={
                            "inner_key1": layabase.database_mongo.Column(
                                EnumTest, is_nullable=False
                            ),
                            "inner_key2": layabase.database_mongo.Column(
                                int, is_nullable=False
                            ),
                        },
                        is_required=True,
                    ),
                    "second_key": layabase.database_mongo.Column(
                        int, is_nullable=False
                    ),
                },
                is_required=True,
            )

        model = TestDictInDictModel

    _db = layabase.load("mongomock", [TestController])
    yield TestController
    layabase.testing.reset(_db)


def test_get_with_dot_notation_multi_level_is_valid(controller):
    assert {
        "dict_field": {
            "first_key": {"inner_key1": "Value1", "inner_key2": 3},
            "second_key": 3,
        },
        "key": "my_key",
    } == controller.post(
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
    } == controller.post(
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
    ] == controller.get({"dict_field.first_key.inner_key1": EnumTest.Value1})
