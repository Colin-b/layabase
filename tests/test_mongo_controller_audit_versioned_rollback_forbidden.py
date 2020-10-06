import enum
from typing import List, Dict

import pytest

import layabase
import layabase.mongo


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        enum_fld = layabase.mongo.Column(EnumTest)

        @classmethod
        def validate_rollback(
            cls, filters: dict, future_documents: List[dict]
        ) -> Dict[str, List[str]]:
            return {"key": ["Rollback forbidden"]}

    controller = layabase.CRUDController(TestCollection, audit=True, history=True)
    layabase.load("mongomock", [controller])
    return controller


def test_rollback_validation_custom(controller: layabase.CRUDController):
    controller.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    controller.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    controller.delete({"key": "my_key"})
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.rollback_to({"revision": 1})
    assert exception_info.value.errors == {"key": ["Rollback forbidden"]}
    assert exception_info.value.received_data == {"revision": 1}
