import datetime
import enum
import re
from typing import List, Dict

import pytest
from flask_restplus import inputs
from pycommon_error.validation import ValidationFailed, ModelCouldNotBeFound

from pycommon_database import database, database_mongo, versioning_mongo
from pycommon_database.database_mongo import _validate_date_time, _validate_int
from test.flask_restplus_mock import TestAPI


def parser_types(flask_parser) -> dict:
    return {arg.name: arg.type for arg in flask_parser.args}


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class TestController(database.CRUDController):
    pass


class TestEnumController(database.CRUDController):
    pass


class TestVersionedController(database.CRUDController):
    pass


class TestVersionedNoRollbackAllowedController(database.CRUDController):
    pass


class TestPrimaryIntController(database.CRUDController):
    pass


class TestIntController(database.CRUDController):
    pass


class TestPrimaryIntVersionedController(database.CRUDController):
    pass


class TestAutoIncAuditVersionedController(database.CRUDController):
    pass


def _create_models(base):
    class TestModel(
        database_mongo.CRUDModel, base=base, table_name="sample_table_name", audit=True
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        mandatory = database_mongo.Column(int, is_nullable=False)
        optional = database_mongo.Column(str)

    class TestEnumModel(
        database_mongo.CRUDModel, base=base, table_name="enum_table_name", audit=True
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        enum_fld = database_mongo.Column(EnumTest)

    class TestPrimaryIntModel(
        database_mongo.CRUDModel,
        base=base,
        table_name="prim_int_table_name",
        audit=True,
    ):
        key = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = database_mongo.Column()

    class TestPrimaryIntVersionedModel(
        versioning_mongo.VersionedCRUDModel,
        base=base,
        table_name="prim_int_version_table_name",
        audit=True,
    ):
        key = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = database_mongo.Column()

    class TestAutoIncAuditVersionedModel(
        versioning_mongo.VersionedCRUDModel,
        base=base,
        table_name="prim_int_auto_inc_version_table_name",
        audit=True,
    ):
        key = database_mongo.Column(
            int, is_primary_key=True, should_auto_increment=True
        )
        other = database_mongo.Column(int)

    class TestIntModel(
        database_mongo.CRUDModel, base=base, table_name="int_table_name", audit=True
    ):
        key = database_mongo.Column(int)

    class TestVersionedModel(
        versioning_mongo.VersionedCRUDModel,
        base=base,
        table_name="versioned_table_name",
        audit=True,
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        enum_fld = database_mongo.Column(EnumTest)

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

    TestController.model(TestModel)
    TestEnumController.model(TestEnumModel)
    TestPrimaryIntController.model(TestPrimaryIntModel)
    TestIntController.model(TestIntModel)
    TestPrimaryIntVersionedController.model(TestPrimaryIntVersionedModel)
    TestVersionedController.model(TestVersionedModel)
    TestVersionedNoRollbackAllowedController.model(TestVersionedNoRollbackAllowedModel)
    TestAutoIncAuditVersionedController.model(TestAutoIncAuditVersionedModel)
    return [
        TestModel,
        TestEnumModel,
        TestPrimaryIntModel,
        TestPrimaryIntVersionedModel,
        TestVersionedModel,
        TestIntModel,
        TestAutoIncAuditVersionedModel,
        TestVersionedNoRollbackAllowedModel,
    ]


@pytest.fixture
def db():
    _db = database.load("mongomock?ssl=True", _create_models, replicaSet="globaldb")
    TestController.namespace(TestAPI)
    TestEnumController.namespace(TestAPI)
    TestVersionedController.namespace(TestAPI)
    TestVersionedNoRollbackAllowedController.namespace(TestAPI)
    TestPrimaryIntController.namespace(TestAPI)
    TestIntController.namespace(TestAPI)
    TestPrimaryIntVersionedController.namespace(TestAPI)
    TestAutoIncAuditVersionedController.namespace(TestAPI)

    yield _db

    database.reset(_db)


def test_get_all_without_data_returns_empty_list(db):
    assert [] == TestController.get({})
    _check_audit(TestController, [])


def test_audit_table_name_is_forbidden(db):
    with pytest.raises(Exception) as exception_info:

        class TestAuditModel(database_mongo.CRUDModel, base=db, table_name="audit"):
            key = database_mongo.Column(str)

    assert "audit is a reserved collection name." == str(exception_info.value)


def test_audited_table_name_is_forbidden(db):
    with pytest.raises(Exception) as exception_info:

        class TestAuditModel(
            database_mongo.CRUDModel, base=db, table_name="audit_int_table_name"
        ):
            key = database_mongo.Column(str)

    assert "audit_int_table_name is a reserved collection name." == str(
        exception_info.value
    )


def test_get_parser_fields_order(db):
    assert {
        "key": str,
        "mandatory": _validate_int,
        "optional": str,
        "limit": inputs.positive,
        "offset": inputs.natural,
    } == parser_types(TestController.query_get_parser)


def test_get_versioned_audit_parser_fields(db):
    assert {
        "audit_action": str,
        "audit_date_utc": _validate_date_time,
        "audit_user": str,
        "limit": inputs.positive,
        "offset": inputs.natural,
        "revision": _validate_int,
    } == parser_types(TestVersionedController.query_get_audit_parser)


def test_delete_parser_fields_order(db):
    assert {"key": str, "mandatory": _validate_int, "optional": str} == parser_types(
        TestController.query_delete_parser
    )


def test_post_model_fields_order(db):
    assert {
        "key": "String",
        "mandatory": "Integer",
        "optional": "String",
    } == TestController.json_post_model.fields_flask_type


def test_versioned_audit_get_response_model_fields(db):
    assert {
        "audit_action": "String",
        "audit_date_utc": "DateTime",
        "audit_user": "String",
        "revision": "Integer",
    } == TestVersionedController.get_audit_response_model.fields_flask_type


def test_put_model_fields_order(db):
    assert {
        "key": "String",
        "mandatory": "Integer",
        "optional": "String",
    } == TestController.json_put_model.fields_flask_type


def test_get_response_model_fields_order(db):
    assert {
        "key": "String",
        "mandatory": "Integer",
        "optional": "String",
    } == TestController.get_response_model.fields_flask_type


def test_post_with_nothing_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert not exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert [] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({})
    assert {
        "key": ["Missing data for required field."],
        "mandatory": ["Missing data for required field."],
    } == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_empty_list_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post_many([])
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert [] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert not exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.put({})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_delete_without_nothing_do_not_fail(db):
    assert 0 == TestController.delete({})
    _check_audit(TestController, [])


def test_post_without_mandatory_field_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        TestController.post({"key": "my_key"})
    assert {
        "mandatory": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "my_key"} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_without_mandatory_field_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many([{"key": "my_key"}])
    assert {
        0: {"mandatory": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [{"key": "my_key"}] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_without_key_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post({"mandatory": 1})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {"mandatory": 1} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_without_key_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many([{"mandatory": 1}])
    assert {
        0: {"key": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [{"mandatory": 1}] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_with_wrong_type_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post({"key": datetime.date(2007, 12, 5), "mandatory": 1})
    assert {"key": ["Not a valid str."]} == exception_info.value.errors
    assert {
        "key": datetime.date(2007, 12, 5),
        "mandatory": 1,
    } == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_wrong_type_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many([{"key": datetime.date(2007, 12, 5), "mandatory": 1}])
    assert {0: {"key": ["Not a valid str."]}} == exception_info.value.errors
    assert [
        {"key": datetime.date(2007, 12, 5), "mandatory": 1}
    ] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_wrong_type_is_invalid(db):
    TestController.post({"key": "value1", "mandatory": 1})
    with pytest.raises(Exception) as exception_info:
        TestController.put({"key": "value1", "mandatory": "invalid_value"})
    assert {"mandatory": ["Not a valid int."]} == exception_info.value.errors
    assert {
        "key": "value1",
        "mandatory": "invalid_value",
    } == exception_info.value.received_data
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "value1",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )


def test_versioned_int_primary_key_is_reset_after_delete(db):
    assert {
        "key": 1,
        "other": "test1",
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    } == TestPrimaryIntVersionedController.post({"other": "test1"})
    assert 1 == TestPrimaryIntVersionedController.delete({})
    assert {
        "key": 1,
        "other": "test1",
        "valid_since_revision": 3,
        "valid_until_revision": -1,
    } == TestPrimaryIntVersionedController.post({"other": "test1"})
    _check_audit(
        TestPrimaryIntVersionedController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 1,
                "table_name": "prim_int_version_table_name",
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 2,
                "table_name": "prim_int_version_table_name",
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 3,
                "table_name": "prim_int_version_table_name",
            },
        ],
    )
    assert [
        {
            "key": 1,
            "other": "test1",
            "valid_since_revision": 1,
            "valid_until_revision": 2,
        },
        {
            "key": 1,
            "other": "test1",
            "valid_since_revision": 3,
            "valid_until_revision": -1,
        },
    ] == TestPrimaryIntVersionedController.get_history({})


def test_auto_incremented_fields_are_not_incremented_on_post_failure(db):
    assert {
        "key": 1,
        "other": 1,
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    } == TestAutoIncAuditVersionedController.post({"other": 1})

    # Should not increment revision, nor the auto incremented key
    with pytest.raises(ValidationFailed):
        TestAutoIncAuditVersionedController.post({"other": "FAILED"})

    assert {
        "key": 2,
        "other": 2,
        "valid_since_revision": 2,
        "valid_until_revision": -1,
    } == TestAutoIncAuditVersionedController.post({"other": 2})


def test_auto_incremented_fields_are_not_incremented_on_multi_post_failure(db):
    assert [
        {"key": 1, "other": 1, "valid_since_revision": 1, "valid_until_revision": -1}
    ] == TestAutoIncAuditVersionedController.post_many([{"other": 1}])

    # Should not increment revision, nor the auto incremented key
    with pytest.raises(ValidationFailed):
        TestAutoIncAuditVersionedController.post_many(
            [{"other": 2}, {"other": "FAILED"}, {"other": 4}]
        )

    assert [
        {
            "key": 3,  # For performance reasons, deserialization is performed before checks on other doc (so first valid document incremented the counter)
            "other": 5,
            "valid_since_revision": 2,
            "valid_until_revision": -1,
        }
    ] == TestAutoIncAuditVersionedController.post_many([{"other": 5}])


def test_auto_incremented_fields_are_not_incremented_on_multi_post_failure(db):
    assert [
        {"key": 1, "other": 1, "valid_since_revision": 1, "valid_until_revision": -1}
    ] == TestAutoIncAuditVersionedController.post_many([{"other": 1}])

    # Should not increment revision
    with pytest.raises(ValidationFailed):
        TestAutoIncAuditVersionedController.put_many(
            [{"other": 1}, {"other": "FAILED"}, {"other": 1}]
        )

    assert [
        {"key": 2, "other": 5, "valid_since_revision": 2, "valid_until_revision": -1}
    ] == TestAutoIncAuditVersionedController.post_many([{"other": 5}])


def test_int_primary_key_is_reset_after_delete(db):
    assert {"key": 1, "other": "test1"} == TestPrimaryIntController.post(
        {"other": "test1"}
    )
    assert 1 == TestPrimaryIntController.delete({})
    assert {"key": 1, "other": "test1"} == TestPrimaryIntController.post(
        {"other": "test1"}
    )
    assert {"key": 2, "other": "test1"} == TestPrimaryIntController.post(
        {"other": "test1"}
    )
    _check_audit(
        TestPrimaryIntController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "other": "test1",
                "revision": 1,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "other": "test1",
                "revision": 2,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "other": "test1",
                "revision": 3,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 2,
                "other": "test1",
                "revision": 4,
            },
        ],
    )


def test_int_revision_is_not_reset_after_delete(db):
    assert {"key": 1} == TestIntController.post({"key": 1})
    assert 1 == TestIntController.delete({})
    assert {"key": 1} == TestIntController.post({"key": 1})
    assert {"key": 2} == TestIntController.post({"key": 2})
    _check_audit(
        TestIntController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "revision": 1,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "revision": 2,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 1,
                "revision": 3,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": 2,
                "revision": 4,
            },
        ],
    )


def test_post_without_optional_is_valid(db):
    assert {"optional": None, "mandatory": 1, "key": "my_key"} == TestController.post(
        {"key": "my_key", "mandatory": 1}
    )
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )


def test_revision_not_shared_if_not_versioned(db):
    assert {"optional": None, "mandatory": 1, "key": "my_key"} == TestController.post(
        {"key": "my_key", "mandatory": 1}
    )
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )
    _check_audit(
        TestVersionedController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 1,
                "table_name": "versioned_table_name",
            }
        ],
    )


def test_post_with_enum_is_valid(db):
    assert {"enum_fld": "Value1", "key": "my_key"} == TestEnumController.post(
        {"key": "my_key", "enum_fld": EnumTest.Value1}
    )
    _check_audit(
        TestEnumController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "enum_fld": "Value1",
                "key": "my_key",
                "revision": 1,
            }
        ],
    )


def test_put_with_enum_is_valid(db):
    assert {"enum_fld": "Value1", "key": "my_key"} == TestEnumController.post(
        {"key": "my_key", "enum_fld": EnumTest.Value1}
    )
    assert (
        {"enum_fld": "Value1", "key": "my_key"},
        {"enum_fld": "Value2", "key": "my_key"},
    ) == TestEnumController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    _check_audit(
        TestEnumController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "enum_fld": "Value1",
                "key": "my_key",
                "revision": 1,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "enum_fld": "Value2",
                "key": "my_key",
                "revision": 2,
            },
        ],
    )


def test_revision_on_versionned_audit_after_put_failure(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    with pytest.raises(ModelCouldNotBeFound):
        TestVersionedController.put({"key": "my_key2", "enum_fld": EnumTest.Value2})
    TestVersionedController.delete({"key": "my_key"})
    _check_audit(
        TestVersionedController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 1,
                "table_name": "versioned_table_name",
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 2,
                "table_name": "versioned_table_name",
            },
        ],
    )


def test_versioned_audit_after_post_put_delete_rollback(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    TestVersionedController.delete({"key": "my_key"})
    TestVersionedController.rollback_to({"revision": 1})
    _check_audit(
        TestVersionedController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 1,
                "table_name": "versioned_table_name",
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 2,
                "table_name": "versioned_table_name",
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 3,
                "table_name": "versioned_table_name",
            },
            {
                "audit_action": "Rollback",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "revision": 4,
                "table_name": "versioned_table_name",
            },
        ],
    )


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


def test_get_last_when_empty(db):
    assert TestVersionedController.get_last({}) == {}


def test_get_last_when_single_doc_post(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    assert TestVersionedController.get_last({}) == {
        "enum_fld": "Value1",
        "key": "my_key",
        "valid_since_revision": 1,
        "valid_until_revision": -1,
    }


def test_get_last_with_unmatched_filter(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    assert TestVersionedController.get_last({"key": "my_key2"}) == {}


def test_get_last_when_single_update(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    assert TestVersionedController.get_last({}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": -1,
    }


def test_get_last_when_removed(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    TestVersionedController.delete({"key": "my_key"})
    assert TestVersionedController.get_last({}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": 3,
    }


def test_get_last_with_one_removed_and_a_valid(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    TestVersionedController.delete({"key": "my_key"})
    TestVersionedController.post({"key": "my_key2", "enum_fld": EnumTest.Value1})
    assert TestVersionedController.get_last({}) == {
        "enum_fld": "Value1",
        "key": "my_key2",
        "valid_since_revision": 4,
        "valid_until_revision": -1,
    }


def test_get_last_with_one_removed_and_a_valid_and_filter_on_removed(db):
    TestVersionedController.post({"key": "my_key", "enum_fld": EnumTest.Value1})
    TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
    TestVersionedController.delete({"key": "my_key"})
    TestVersionedController.post({"key": "my_key2", "enum_fld": EnumTest.Value1})
    assert TestVersionedController.get_last({"key": "my_key"}) == {
        "enum_fld": "Value2",
        "key": "my_key",
        "valid_since_revision": 2,
        "valid_until_revision": 3,
    }


def test_delete_with_enum_is_valid(db):
    assert {"enum_fld": "Value1", "key": "my_key"} == TestEnumController.post(
        {"key": "my_key", "enum_fld": EnumTest.Value1}
    )
    assert 1 == TestEnumController.delete({"enum_fld": EnumTest.Value1})
    _check_audit(
        TestEnumController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "enum_fld": "Value1",
                "key": "my_key",
                "revision": 1,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "enum_fld": "Value1",
                "key": "my_key",
                "revision": 2,
            },
        ],
    )


def test_post_many_without_optional_is_valid(db):
    assert [
        {"optional": None, "mandatory": 1, "key": "my_key"}
    ] == TestController.post_many([{"key": "my_key", "mandatory": 1}])
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )


def test_put_many_is_valid(db):
    TestController.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )
    TestController.put_many(
        [{"key": "my_key", "optional": "test"}, {"key": "my_key2", "mandatory": 3}]
    )
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": None,
                "revision": 2,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": "test",
                "revision": 3,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 3,
                "optional": None,
                "revision": 4,
            },
        ],
    )


def _check_audit(controller, expected_audit, filter_audit={}):
    audit = controller.get_audit(filter_audit)
    audit = [
        {key: audit_line[key] for key in sorted(audit_line.keys())}
        for audit_line in audit
    ]

    if not expected_audit:
        assert audit == expected_audit
    else:
        assert re.match(
            f"{expected_audit}".replace("[", "\\[")
            .replace("]", "\\]")
            .replace("\\\\", "\\"),
            f"{audit}",
        )


def test_post_with_optional_is_valid(db):
    assert {
        "mandatory": 1,
        "key": "my_key",
        "optional": "my_value",
    } == TestController.post({"key": "my_key", "mandatory": 1, "optional": "my_value"})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 1,
            }
        ],
    )


def test_post_many_with_optional_is_valid(db):
    assert [
        {"mandatory": 1, "key": "my_key", "optional": "my_value"}
    ] == TestController.post_many(
        [{"key": "my_key", "mandatory": 1, "optional": "my_value"}]
    )
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 1,
            }
        ],
    )


def test_post_with_unknown_field_is_valid(db):
    assert {
        "optional": "my_value",
        "mandatory": 1,
        "key": "my_key",
    } == TestController.post(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    )
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 1,
            }
        ],
    )


def test_post_many_with_unknown_field_is_valid(db):
    assert [
        {"optional": "my_value", "mandatory": 1, "key": "my_key"}
    ] == TestController.post_many(
        [
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            }
        ]
    )
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 1,
            }
        ],
    )


def test_get_without_filter_is_retrieving_the_only_item(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert [
        {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
    ] == TestController.get({})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            }
        ],
    )


def test_get_without_filter_is_retrieving_everything_with_multiple_posts(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
        ],
    )


def test_get_without_filter_is_retrieving_everything(db):
    TestController.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestController.get({})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
        ],
    )


def test_get_with_filter_is_retrieving_subset(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == TestController.get({"optional": "my_value1"})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
        ],
    )


def test_put_is_updating(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    ) == TestController.put({"key": "my_key1", "optional": "my_value"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ] == TestController.get({"mandatory": 1})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 2,
            },
        ],
    )


def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.put({"key": "my_key1", "optional": "my_value"})
    assert [] == TestController.get({"optional": "my_value1"})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value",
                "revision": 2,
            },
        ],
    )


def test_delete_with_filter_is_removing_the_proper_row(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 1 == TestController.delete({"key": "my_key1"})
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ] == TestController.get({})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 3,
            },
        ],
    )


def test_audit_filter_on_model_is_returning_only_selected_data(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.put({"key": "my_key1", "mandatory": 2})
    TestController.delete({"key": "my_key1"})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 2,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 3,
            },
        ],
        filter_audit={"key": "my_key1"},
    )


def test_audit_filter_on_audit_model_is_returning_only_selected_data(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.put({"key": "my_key1", "mandatory": 2})
    TestController.delete({"key": "my_key1"})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 2,
            }
        ],
        filter_audit={"audit_action": "Update"},
    )


def test_value_can_be_updated_to_previous_value(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.put({"key": "my_key1", "mandatory": 2})
    TestController.put({"key": "my_key1", "mandatory": 1})  # Put back initial value
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 2,
            },
            {
                "audit_action": "Update",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 3,
            },
        ],
    )


def test_delete_without_filter_is_removing_everything(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == TestController.delete({})
    assert [] == TestController.get({})
    _check_audit(
        TestController,
        [
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "Insert",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 3,
            },
            {
                "audit_action": "Delete",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 4,
            },
        ],
    )


def test_query_get_parser(db):
    assert {
        "key": str,
        "mandatory": _validate_int,
        "optional": str,
        "limit": inputs.positive,
        "offset": inputs.natural,
    } == parser_types(TestController.query_get_parser)
    _check_audit(TestController, [])


def test_query_get_audit_parser(db):
    assert {
        "audit_action": str,
        "audit_date_utc": _validate_date_time,
        "audit_user": str,
        "key": str,
        "mandatory": _validate_int,
        "optional": str,
        "limit": inputs.positive,
        "offset": inputs.natural,
        "revision": _validate_int,
    } == parser_types(TestController.query_get_audit_parser)
    _check_audit(TestController, [])


def test_query_delete_parser(db):
    assert {"key": str, "mandatory": _validate_int, "optional": str} == parser_types(
        TestController.query_delete_parser
    )
    _check_audit(TestController, [])


def test_get_response_model(db):
    assert "TestModel" == TestController.get_response_model.name
    assert {
        "key": "String",
        "mandatory": "Integer",
        "optional": "String",
    } == TestController.get_response_model.fields_flask_type
    _check_audit(TestController, [])


def test_get_audit_response_model(db):
    assert "AuditTestModel" == TestController.get_audit_response_model.name
    assert {
        "audit_action": "String",
        "audit_date_utc": "DateTime",
        "audit_user": "String",
        "key": "String",
        "mandatory": "Integer",
        "optional": "String",
        "revision": "Integer",
    } == TestController.get_audit_response_model.fields_flask_type
    _check_audit(TestController, [])
