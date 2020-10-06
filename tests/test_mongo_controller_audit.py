import datetime

import pytest

import layabase
import layabase.mongo
from layabase.testing import mock_mongo_audit_datetime


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        mandatory = layabase.mongo.Column(int, is_nullable=False)
        optional = layabase.mongo.Column(str)

    controller = layabase.CRUDController(TestCollection, audit=True)
    layabase.load("mongomock?ssl=True", [controller], replicaSet="globaldb")
    return controller


def test_get_all_without_data_returns_empty_list(controller: layabase.CRUDController):
    assert controller.get({}) == []
    assert controller.get_audit({}) == []


def test_audit_table_name_is_forbidden():
    class TestCollection:
        __collection_name__ = "audit"

        key = layabase.mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        layabase.load(
            "mongomock?ssl=True",
            [layabase.CRUDController(TestCollection)],
            replicaSet="globaldb",
        )

    assert "audit is a reserved collection name." == str(exception_info.value)


def test_audited_table_name_is_forbidden():
    class TestCollection:
        __collection_name__ = "audit_test"

        key = layabase.mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        layabase.load(
            "mongomock?ssl=True",
            [layabase.CRUDController(TestCollection)],
            replicaSet="globaldb",
        )

    assert str(exception_info.value) == "audit_test is a reserved collection name."


def test_post_with_nothing_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert not exception_info.value.received_data
    assert controller.get_audit({}) == []


def test_post_many_with_nothing_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == []
    assert controller.get_audit({}) == []


def test_post_with_empty_dict_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({})
    assert {
        "key": ["Missing data for required field."],
        "mandatory": ["Missing data for required field."],
    } == exception_info.value.errors
    assert exception_info.value.received_data == {}
    assert controller.get_audit({}) == []


def test_post_many_with_empty_list_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many([])
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == []
    assert controller.get_audit({}) == []


def test_put_with_nothing_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert not exception_info.value.received_data
    assert controller.get_audit({}) == []


def test_put_with_empty_dict_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({})
    assert exception_info.value.errors == {"key": ["Missing data for required field."]}
    assert exception_info.value.received_data == {}
    assert controller.get_audit({}) == []


def test_delete_without_nothing_do_not_fail(controller: layabase.CRUDController):
    assert controller.delete({}) == 0
    assert controller.get_audit({}) == []


def test_post_without_mandatory_field_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"key": "my_key"})
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }
    assert exception_info.value.received_data == {"key": "my_key"}
    assert controller.get_audit({}) == []


def test_post_many_without_mandatory_field_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many([{"key": "my_key"}])
    assert exception_info.value.errors == {
        0: {"mandatory": ["Missing data for required field."]}
    }
    assert exception_info.value.received_data == [{"key": "my_key"}]
    assert controller.get_audit({}) == []


def test_post_without_key_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"mandatory": 1})
    assert exception_info.value.errors == {"key": ["Missing data for required field."]}
    assert exception_info.value.received_data == {"mandatory": 1}
    assert controller.get_audit({}) == []


def test_post_many_without_key_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many([{"mandatory": 1}])
    assert exception_info.value.errors == {
        0: {"key": ["Missing data for required field."]}
    }
    assert exception_info.value.received_data == [{"mandatory": 1}]
    assert controller.get_audit({}) == []


def test_post_with_wrong_type_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"key": datetime.date(2007, 12, 5), "mandatory": 1})
    assert exception_info.value.errors == {"key": ["Not a valid str."]}
    assert exception_info.value.received_data == {
        "key": datetime.date(2007, 12, 5),
        "mandatory": 1,
    }
    assert controller.get_audit({}) == []


def test_post_many_with_wrong_type_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many([{"key": datetime.date(2007, 12, 5), "mandatory": 1}])
    assert exception_info.value.errors == {0: {"key": ["Not a valid str."]}}
    assert exception_info.value.received_data == [
        {"key": datetime.date(2007, 12, 5), "mandatory": 1}
    ]
    assert controller.get_audit({}) == []


def test_put_with_wrong_type_is_invalid(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    controller.post({"key": "value1", "mandatory": 1})
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({"key": "value1", "mandatory": "invalid_value"})
    assert exception_info.value.errors == {"mandatory": ["Not a valid int."]}
    assert exception_info.value.received_data == {
        "key": "value1",
        "mandatory": "invalid_value",
    }
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "value1",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_post_without_optional_is_valid(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    assert controller.post({"key": "my_key", "mandatory": 1}) == {
        "optional": None,
        "mandatory": 1,
        "key": "my_key",
    }
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_post_many_without_optional_is_valid(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    assert controller.post_many([{"key": "my_key", "mandatory": 1}]) == [
        {"optional": None, "mandatory": 1, "key": "my_key"}
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        }
    ]


def test_put_many_is_valid(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    controller.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )
    controller.put_many(
        [{"key": "my_key", "optional": "test"}, {"key": "my_key2", "mandatory": 3}]
    )
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": None,
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": None,
            "revision": 2,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "test",
            "revision": 3,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 3,
            "optional": None,
            "revision": 4,
        },
    ]


def test_post_with_optional_is_valid(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    assert controller.post(
        {"key": "my_key", "mandatory": 1, "optional": "my_value"}
    ) == {"mandatory": 1, "key": "my_key", "optional": "my_value"}
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_many_with_optional_is_valid(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    assert controller.post_many(
        [{"key": "my_key", "mandatory": 1, "optional": "my_value"}]
    ) == [{"mandatory": 1, "key": "my_key", "optional": "my_value"}]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_with_unknown_field_is_valid(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    assert controller.post(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    ) == {"optional": "my_value", "mandatory": 1, "key": "my_key"}
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_post_many_with_unknown_field_is_valid(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    assert controller.post_many(
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
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 1,
        }
    ]


def test_get_without_filter_is_retrieving_the_only_item(
    controller: layabase.CRUDController, mock_mongo_audit_datetime
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller.get({}) == [
        {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        }
    ]


def test_get_without_filter_is_retrieving_everything_with_multiple_posts(
    controller: layabase.CRUDController, mock_mongo_audit_datetime
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_get_without_filter_is_retrieving_everything(
    controller: layabase.CRUDController, mock_mongo_audit_datetime
):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert controller.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_get_with_filter_is_retrieving_subset(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({"optional": "my_value1"}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
    ]


def test_put_is_updating(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller.put({"key": "my_key1", "optional": "my_value"}) == (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    )
    assert controller.get({"mandatory": 1}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 2,
        },
    ]


def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(
    controller: layabase.CRUDController, mock_mongo_audit_datetime
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "optional": "my_value"})
    assert controller.get({"optional": "my_value1"}) == []
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value",
            "revision": 2,
        },
    ]


def test_delete_with_filter_is_removing_the_proper_row(
    controller: layabase.CRUDController, mock_mongo_audit_datetime
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.delete({"key": "my_key1"}) == 1
    assert controller.get({}) == [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ]
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 3,
        },
    ]


def test_audit_filter_is_returning_only_selected_data(
    controller: layabase.CRUDController, mock_mongo_audit_datetime
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "mandatory": 2})
    controller.delete({"key": "my_key1"})
    assert controller.get_audit({"key": "my_key1"}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 2,
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 3,
        },
    ]


def test_audit_filter_on_audit_collection_is_returning_only_selected_data(
    controller: layabase.CRUDController, mock_mongo_audit_datetime
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "mandatory": 2})
    controller.delete({"key": "my_key1"})
    assert controller.get_audit({"audit_action": "Update"}) == [
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 2,
        }
    ]


def test_value_can_be_updated_to_previous_value(controller: layabase.CRUDController, mock_mongo_audit_datetime):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "mandatory": 2})
    controller.put({"key": "my_key1", "mandatory": 1})  # Put back initial value
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 2,
            "optional": "my_value1",
            "revision": 2,
        },
        {
            "audit_action": "Update",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 3,
        },
    ]


def test_update_index(controller: layabase.CRUDController):
    # Assert no error is thrown
    controller._model.update_indexes()


def test_delete_without_filter_is_removing_everything(
    controller: layabase.CRUDController, mock_mongo_audit_datetime
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == controller.delete({})
    assert [] == controller.get({})
    assert controller.get_audit({}) == [
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 1,
        },
        {
            "audit_action": "Insert",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 2,
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key1",
            "mandatory": 1,
            "optional": "my_value1",
            "revision": 3,
        },
        {
            "audit_action": "Delete",
            "audit_date_utc": "2018-10-11T15:05:05.663000",
            "audit_user": "",
            "key": "my_key2",
            "mandatory": 2,
            "optional": "my_value2",
            "revision": 4,
        },
    ]
