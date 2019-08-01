import re

import pytest
import sqlalchemy
from flask_restplus import inputs

from layabase import database, database_sqlalchemy
from test.flask_restplus_mock import TestAPI


def parser_types(flask_parser) -> dict:
    return {arg.name: arg.type for arg in flask_parser.args}


class TestController(database.CRUDController):
    pass


class Test2Controller(database.CRUDController):
    pass


def _create_models(base):
    class TestModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "sample_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    TestModel.audit()

    class Test2Model(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "sample2_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    Test2Model.audit()

    TestController.model(TestModel)
    Test2Controller.model(Test2Model)
    return [TestModel, Test2Model]


@pytest.fixture
def db():
    _db = database.load("sqlite:///:memory:", _create_models)
    TestController.namespace(TestAPI)
    Test2Controller.namespace(TestAPI)
    yield _db
    database.reset(_db)


def test_get_all_without_data_returns_empty_list(db):
    assert [] == TestController.get({})
    _check_audit(TestController, [])


def test_get_parser_fields_order(db):
    assert ["key", "mandatory", "optional", "limit", "order_by", "offset"] == [
        arg.name for arg in TestController.query_get_parser.args
    ]


def test_delete_parser_fields_order(db):
    assert ["key", "mandatory", "optional"] == [
        arg.name for arg in TestController.query_delete_parser.args
    ]


def test_post_model_fields_order(db):
    assert {
        "key": "String",
        "mandatory": "Integer",
        "optional": "String",
    } == TestController.json_post_model.fields_flask_type


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
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_nothing_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_with_empty_dict_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_empty_list_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many([])
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_nothing_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.put(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_empty_dict_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.put({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_delete_without_nothing_do_not_fail(db):
    assert 0 == TestController.delete({})
    _check_audit(TestController, [])


def test_post_without_mandatory_field_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
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
        TestController.post({"key": 256, "mandatory": 1})
    assert {"key": ["Not a valid string."]} == exception_info.value.errors
    assert {"key": 256, "mandatory": 1} == exception_info.value.received_data
    _check_audit(TestController, [])


def test_post_many_with_wrong_type_is_invalid(db):
    with pytest.raises(Exception) as exception_info:
        TestController.post_many([{"key": 256, "mandatory": 1}])
    assert {0: {"key": ["Not a valid string."]}} == exception_info.value.errors
    assert [{"key": 256, "mandatory": 1}] == exception_info.value.received_data
    _check_audit(TestController, [])


def test_put_with_wrong_type_is_invalid(db):
    TestController.post({"key": "value1", "mandatory": 1})
    with pytest.raises(Exception) as exception_info:
        TestController.put({"key": "value1", "mandatory": "invalid_value"})
    assert {"mandatory": ["Not a valid integer."]} == exception_info.value.errors
    assert {
        "key": "value1",
        "mandatory": "invalid_value",
    } == exception_info.value.received_data
    _check_audit(
        TestController,
        [
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "value1",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )


def test_post_on_a_second_model_without_optional_is_valid(db):
    TestController.post({"key": "my_key", "mandatory": 1})
    assert {"optional": None, "mandatory": 1, "key": "my_key"} == Test2Controller.post(
        {"key": "my_key", "mandatory": 1}
    )
    _check_audit(
        Test2Controller,
        [
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )
    _check_audit(
        TestController,
        [
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key",
                "mandatory": 1,
                "optional": None,
                "revision": 1,
            }
        ],
    )


def _check_audit(controller, expected_audit, filter_audit=None):
    if not filter_audit:
        filter_audit = {}
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "U",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "U",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
            {
                "audit_action": "D",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "U",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 2,
            },
            {
                "audit_action": "D",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "audit_action": "U",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 2,
                "optional": "my_value1",
                "revision": 2,
            }
        ],
        filter_audit={"audit_action": "U"},
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
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 1,
            },
            {
                "audit_action": "I",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                "revision": 2,
            },
            {
                "audit_action": "D",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                "audit_user": "",
                "key": "my_key1",
                "mandatory": 1,
                "optional": "my_value1",
                "revision": 3,
            },
            {
                "audit_action": "D",
                "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
        "mandatory": int,
        "optional": str,
        "limit": inputs.positive,
        "order_by": str,
        "offset": inputs.natural,
    } == parser_types(TestController.query_get_parser)
    _check_audit(TestController, [])


def test_query_get_audit_parser(db):
    assert {
        "audit_action": str,
        "audit_date_utc": inputs.datetime_from_iso8601,
        "audit_user": str,
        "key": str,
        "mandatory": int,
        "optional": str,
        "limit": inputs.positive,
        "order_by": str,
        "offset": inputs.natural,
        "revision": int,
    } == parser_types(TestController.query_get_audit_parser)
    _check_audit(TestController, [])


def test_query_delete_parser(db):
    assert {"key": str, "mandatory": int, "optional": str} == parser_types(
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
