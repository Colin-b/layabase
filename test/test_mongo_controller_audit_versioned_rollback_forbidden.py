import enum
from typing import List, Dict

import pytest
from layaberr import ValidationFailed

import layabase
import layabase.database_mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller():
    class TestModel:
        __tablename__ = "test"

        key = layabase.database_mongo.Column(str, is_primary_key=True)
        enum_fld = layabase.database_mongo.Column(EnumTest)

        @classmethod
        def validate_rollback(
            cls, filters: dict, future_documents: List[dict]
        ) -> Dict[str, List[str]]:
            return {"key": ["Rollback forbidden"]}

    controller = layabase.CRUDController(TestModel, audit=True, history=True)
    layabase.load("mongomock", [controller])
    return controller


def test_rollback_validation_custom(controller):
    controller.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    controller.delete({"key": "my_key"})
    with pytest.raises(ValidationFailed) as exception_info:
        controller.rollback_to({"revision": 1})
    assert exception_info.value.errors == {"key": ["Rollback forbidden"]}
    assert exception_info.value.received_data == {"revision": 1}
