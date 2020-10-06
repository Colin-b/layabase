import pytest
import sqlalchemy

import layabase
import layabase._database_sqlalchemy
from layabase.testing import mock_sqlalchemy_audit_datetime


@pytest.fixture
def controller1() -> layabase.CRUDController:
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    return layabase.CRUDController(TestTable, audit=True)


@pytest.fixture
def controller2() -> layabase.CRUDController:
    class TestTable2:
        __tablename__ = "test2"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    return layabase.CRUDController(TestTable2, audit=True)


@pytest.fixture
def controllers(controller1: layabase.CRUDController, controller2: layabase.CRUDController):
    return layabase.load("sqlite:///:memory:", [controller1, controller2])


def test_get_audit_without_providing_a_dictionary(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.get_audit("")
    assert exception_info.value.errors == {"": ["Must be a dictionary."]}
    assert exception_info.value.received_data == ""


def test_get_all_without_data_returns_empty_list(controllers, controller1):
    assert controller1.get({}) == []
    assert controller1.get_audit({}) == []


def test_query_without_being_in_memory(monkeypatch):
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    controller = layabase.CRUDController(TestTable, audit=True)
    monkeypatch.setattr(
        layabase._database_sqlalchemy, "_in_memory", lambda *args: False
    )
    layabase.load("sqlite:///:memory:", [controller])

    # Assert that connection still works
    assert controller.get({}) == []


def test_post_with_nothing_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.post(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_post_many_with_nothing_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.post_many(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_post_with_empty_dict_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.post({})
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_post_many_with_empty_list_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.post_many([])
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_put_with_nothing_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.put(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_put_with_empty_dict_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.put({})
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}
    assert controller1.get_audit({}) == []


def test_delete_without_nothing_do_not_fail(controllers, controller1):
    assert controller1.delete({}) == 0
    assert controller1.get_audit({}) == []


def test_post_without_mandatory_field_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.post({"key": "my_key"})
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }
    assert exception_info.value.received_data == {"key": "my_key"}
    assert controller1.get_audit({}) == []


def test_post_many_without_mandatory_field_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.post_many([{"key": "my_key"}])
    assert exception_info.value.errors == {
        0: {"mandatory": ["Missing data for required field."]}
    }
    assert exception_info.value.received_data == [{"key": "my_key"}]
    assert controller1.get_audit({}) == []


def test_post_without_key_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.post({"mandatory": 1})
    assert exception_info.value.errors == {"key": ["Missing data for required field."]}
    assert exception_info.value.received_data == {"mandatory": 1}
    assert controller1.get_audit({}) == []


def test_post_many_without_key_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.post_many([{"mandatory": 1}])
    assert exception_info.value.errors == {
        0: {"key": ["Missing data for required field."]}
    }
    assert exception_info.value.received_data == [{"mandatory": 1}]
    assert controller1.get_audit({}) == []


def test_post_with_wrong_type_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.post({"key": 256, "mandatory": 1})
    assert exception_info.value.errors == {"key": ["Not a valid string."]}
    assert exception_info.value.received_data == {"key": 256, "mandatory": 1}
    assert controller1.get_audit({}) == []


def test_post_many_with_wrong_type_is_invalid(controllers, controller1):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.post_many([{"key": 256, "mandatory": 1}])
    assert exception_info.value.errors == {0: {"key": ["Not a valid string."]}}
    assert exception_info.value.received_data == [{"key": 256, "mandatory": 1}]
    assert controller1.get_audit({}) == []


def test_put_with_wrong_type_is_invalid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "value1", "mandatory": 1})
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller1.put({"key": "value1", "mandatory": "invalid_value"})
    assert exception_info.value.errors == {"mandatory": ["Not a valid integer."]}
    assert exception_info.value.received_data == {
        "key": "value1",
        "mandatory": "invalid_value",
    }
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "value1",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_post_without_optional_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert controller1.post({"key": "my_key", "mandatory": 1}) == {
        "optional": None,
        "mandatory": 1,
        "key": "my_key",
    }
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_post_on_a_second_table_without_optional_is_valid(
    controllers, controller1, controller2, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key", "mandatory": 1})
    assert controller2.post({"key": "my_key", "mandatory": 1}) == {
        "optional": None,
        "mandatory": 1,
        "key": "my_key",
    }
    assert controller2.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_post_many_without_optional_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert controller1.post_many([{"key": "my_key", "mandatory": 1}]) == [
        {"optional": None, "mandatory": 1, "key": "my_key"}
    ]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_post_with_optional_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert controller1.post(
        {"key": "my_key", "mandatory": 1, "optional": "my_value"}
    ) == {"mandatory": 1, "key": "my_key", "optional": "my_value"}
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_many_with_optional_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert controller1.post_many(
        [{"key": "my_key", "mandatory": 1, "optional": "my_value"}]
    ) == [{"mandatory": 1, "key": "my_key", "optional": "my_value"}]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_with_unknown_field_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert controller1.post(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    ) == {"optional": "my_value", "mandatory": 1, "key": "my_key"}
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_many_with_unknown_field_is_valid(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    assert controller1.post_many(
        [
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            }
        ]
    ) == [{"optional": "my_value", "mandatory": 1, "key": "my_key"}]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_get_without_filter_is_retrieving_the_only_item(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller1.get({}) == [
        {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
    ]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        }
    ]


def test_get_without_filter_is_retrieving_everything_with_multiple_posts(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller1.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_get_without_filter_is_retrieving_everything(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert controller1.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_get_with_filter_is_retrieving_subset(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller1.get({"optional": "my_value1"}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_put_is_updating(controllers, controller1, mock_sqlalchemy_audit_datetime):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller1.put({"key": "my_key1", "optional": "my_value"}) == (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    )
    assert controller1.get({"mandatory": 1}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "U",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 2,
        },
    ]


def test_put_many_is_updating(controllers, controller1, mock_sqlalchemy_audit_datetime):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller1.put_many([{"key": "my_key1", "optional": "my_value"}]) == (
        [{"key": "my_key1", "mandatory": 1, "optional": "my_value1"}],
        [{"key": "my_key1", "mandatory": 1, "optional": "my_value"}],
    )
    assert controller1.get({"mandatory": 1}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "U",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 2,
        },
    ]


def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.put({"key": "my_key1", "optional": "my_value"})
    assert controller1.get({"optional": "my_value1"}) == []
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "U",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 2,
        },
    ]


def test_delete_with_filter_is_removing_the_proper_row(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller1.delete({"key": "my_key1"}) == 1
    assert controller1.get({}) == [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ]
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
        {
            "audit_action": "D",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 3,
        },
    ]


def test_audit_filter_is_returning_only_selected_data(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.put({"key": "my_key1", "mandatory": 2})
    controller1.delete({"key": "my_key1"})
    assert controller1.get_audit({"key": "my_key1"}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "U",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 2,
        },
        {
            "audit_action": "D",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 3,
        },
    ]


def test_audit_filter_on_audit_table_is_returning_only_selected_data(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.put({"key": "my_key1", "mandatory": 2})
    controller1.delete({"key": "my_key1"})
    assert controller1.get_audit({"audit_action": "U"}) == [
        {
            "audit_action": "U",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 2,
        }
    ]


def test_delete_without_filter_is_removing_everything(
    controllers, controller1, mock_sqlalchemy_audit_datetime
):
    controller1.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller1.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller1.delete({}) == 2
    assert controller1.get({}) == []
    assert controller1.get_audit({}) == [
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "I",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
        {
            "audit_action": "D",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 3,
        },
        {
            "audit_action": "D",
            "audit_date_utc": "2018-10-11T15:05:05.663979",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 4,
        },
    ]
