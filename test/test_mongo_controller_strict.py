import pytest
from layaberr import ValidationFailed

import layabase
import layabase.database_mongo
import layabase.testing


@pytest.fixture
def controller():
    class TestStrictModel:
        __tablename__ = "strict_table_name"

        key = layabase.database_mongo.Column(str, is_primary_key=True)
        mandatory = layabase.database_mongo.Column(int, is_nullable=False)
        optional = layabase.database_mongo.Column(str)

    controller = layabase.CRUDController(TestStrictModel, skip_unknown_fields=False)
    _db = layabase.load("mongomock", [controller])
    yield controller
    layabase.testing.reset(_db)


def test_post_with_unknown_field_is_invalid(controller):
    with pytest.raises(ValidationFailed) as exception_info:
        controller.post(
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            }
        )
    assert exception_info.value.errors == {"unknown": ["Unknown field"]}
    assert exception_info.value.received_data == {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
        "unknown": "my_value",
    }
