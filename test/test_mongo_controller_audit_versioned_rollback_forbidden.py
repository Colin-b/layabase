import enum
from typing import List, Dict

import pytest
from layaberr import ValidationFailed

from layabase import database, database_mongo, versioning_mongo
import layabase.testing


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestVersionedNoRollbackAllowedController(database.CRUDController):
    pass


def _create_models(base):
    class TestVersionedNoRollbackAllowedModel(
        versioning_mongo.VersionedCRUDModel,
        base=base,
        table_name="versioned_no_rollback_table_name",
        audit=True,
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        enum_fld = database_mongo.Column(EnumTest)

        @classmethod
        def validate_rollback(
            cls, filters: dict, future_documents: List[dict]
        ) -> Dict[str, List[str]]:
            return {"key": ["Rollback forbidden"]}

    TestVersionedNoRollbackAllowedController.model(TestVersionedNoRollbackAllowedModel)
    return [TestVersionedNoRollbackAllowedModel]


@pytest.fixture
def db():
    _db = database.load("mongomock?ssl=True", _create_models, replicaSet="globaldb")
    yield _db
    layabase.testing.reset(_db)


def test_rollback_validation_custom(db):
    TestVersionedNoRollbackAllowedController.post(
        {"key": "my_key", "enum_fld": EnumTest.Value1}
    )
    TestVersionedNoRollbackAllowedController.put(
        {"key": "my_key", "enum_fld": EnumTest.Value2}
    )
    TestVersionedNoRollbackAllowedController.delete({"key": "my_key"})
    with pytest.raises(ValidationFailed) as exception_info:
        TestVersionedNoRollbackAllowedController.rollback_to({"revision": 1})
    assert {"key": ["Rollback forbidden"]} == exception_info.value.errors
    assert {"revision": 1} == exception_info.value.received_data
