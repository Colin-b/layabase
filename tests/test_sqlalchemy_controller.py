from threading import Thread

import pytest
import sqlalchemy

import layabase


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    controller = layabase.CRUDController(TestTable)
    layabase.load("sqlite:///:memory:", [controller])
    return controller


def test_get_without_providing_a_dictionary(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.get("")
    assert exception_info.value.errors == {"": ["Must be a dictionary."]}
    assert exception_info.value.received_data == ""


def test_get_one_without_providing_a_dictionary(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.get_one("")
    assert exception_info.value.errors == {"": ["Must be a dictionary."]}
    assert exception_info.value.received_data == ""


def test_get_last_without_providing_a_dictionary(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.get_last("")
    assert exception_info.value.errors == {"": ["Must be a dictionary."]}
    assert exception_info.value.received_data == ""


def test_get_history_without_providing_a_dictionary(
    controller: layabase.CRUDController,
):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.get_history("")
    assert exception_info.value.errors == {"": ["Must be a dictionary."]}
    assert exception_info.value.received_data == ""


def test_delete_without_providing_a_dictionary(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.delete("")
    assert exception_info.value.errors == {"": ["Must be a dictionary."]}
    assert exception_info.value.received_data == ""


def test_rollback_without_providing_a_dictionary(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.rollback_to("")
    assert exception_info.value.errors == {"": ["Must be a dictionary."]}
    assert exception_info.value.received_data == ""


def test_get_all_without_data_returns_empty_list(controller: layabase.CRUDController):
    assert controller.get({}) == []


def test_get_one_without_data_returns_empty_dict(controller: layabase.CRUDController):
    assert controller.get_one({}) == {}


def test_post_with_nothing_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}


def test_post_list_with_nothing_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}


def test_post_many_with_empty_dict_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many({})
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}


def test_get_field_names(controller: layabase.CRUDController):
    assert controller.get_field_names() == ["key", "mandatory", "optional"]


def test_primary_keys_are_returned(controller: layabase.CRUDController):
    inserted = controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert (
        controller.get_url("/test", *inserted)
        == "/test?key=my_key1&mandatory=1&key=my_key2&mandatory=2"
    )


def test_dbapierror_when_inserting_many(
    controller: layabase.CRUDController, monkeypatch
):
    def raise_dbapi_error(*args):
        import sqlalchemy.orm.exc

        raise sqlalchemy.orm.exc.sa_exc.DBAPIError(
            "SELECT * FROM test", params={}, orig="orig test"
        )

    monkeypatch.setattr(controller._model._session, "add_all", raise_dbapi_error)
    with pytest.raises(Exception) as exception_info:
        controller.post_many(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ]
        )
    assert (
        str(exception_info.value)
        == "A error occurred while querying database: (builtins.str) orig test\n[SQL: SELECT * FROM test]\n(Background on this error at: http://sqlalche.me/e/dbapi)"
    )


def test_exception_when_inserting_many(
    controller: layabase.CRUDController, monkeypatch
):
    def raise_exception(*args):
        raise Exception("This is the error message")

    monkeypatch.setattr(controller._model._session, "add_all", raise_exception)
    with pytest.raises(Exception) as exception_info:
        controller.post_many(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ]
        )
    assert str(exception_info.value) == "This is the error message"


def test_dbapierror_when_inserting_one(
    controller: layabase.CRUDController, monkeypatch
):
    def raise_dbapi_error(*args):
        import sqlalchemy.orm.exc

        raise sqlalchemy.orm.exc.sa_exc.DBAPIError(
            "SELECT * FROM test", params={}, orig="orig test"
        )

    monkeypatch.setattr(controller._model._session, "add", raise_dbapi_error)
    with pytest.raises(Exception) as exception_info:
        controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert (
        str(exception_info.value)
        == "A error occurred while querying database: (builtins.str) orig test\n[SQL: SELECT * FROM test]\n(Background on this error at: http://sqlalche.me/e/dbapi)"
    )


def test_exception_when_inserting_one(controller: layabase.CRUDController, monkeypatch):
    def raise_exception(*args):
        raise Exception("This is the error message")

    monkeypatch.setattr(controller._model._session, "add", raise_exception)
    with pytest.raises(Exception) as exception_info:
        controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert str(exception_info.value) == "This is the error message"


def test_dbapierror_when_updating_many_retrieval(
    controller: layabase.CRUDController, monkeypatch
):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )

    def raise_dbapi_error(*args):
        import sqlalchemy.orm.exc

        raise sqlalchemy.orm.exc.sa_exc.DBAPIError(
            "SELECT * FROM test", params={}, orig="orig test"
        )

    monkeypatch.setattr(controller._model._session, "query", raise_dbapi_error)
    with pytest.raises(Exception) as exception_info:
        controller.put_many(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ]
        )
    assert (
        str(exception_info.value)
        == "A error occurred while querying database: (builtins.str) orig test\n[SQL: SELECT * FROM test]\n(Background on this error at: http://sqlalche.me/e/dbapi)"
    )


def test_dbapierror_when_updating_many(
    controller: layabase.CRUDController, monkeypatch
):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )

    def raise_dbapi_error(*args):
        import sqlalchemy.orm.exc

        raise sqlalchemy.orm.exc.sa_exc.DBAPIError(
            "SELECT * FROM test", params={}, orig="orig test"
        )

    monkeypatch.setattr(controller._model._session, "add_all", raise_dbapi_error)
    with pytest.raises(Exception) as exception_info:
        controller.put_many(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ]
        )
    assert (
        str(exception_info.value)
        == "A error occurred while querying database: (builtins.str) orig test\n[SQL: SELECT * FROM test]\n(Background on this error at: http://sqlalche.me/e/dbapi)"
    )


def test_exception_when_updating_many(controller: layabase.CRUDController, monkeypatch):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )

    def raise_exception(*args):
        raise Exception("This is the error message")

    monkeypatch.setattr(controller._model._session, "add_all", raise_exception)
    with pytest.raises(Exception) as exception_info:
        controller.put_many(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ]
        )
    assert str(exception_info.value) == "This is the error message"


def test_dbapierror_when_updating_one(controller: layabase.CRUDController, monkeypatch):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )

    def raise_dbapi_error(*args):
        import sqlalchemy.orm.exc

        raise sqlalchemy.orm.exc.sa_exc.DBAPIError(
            "SELECT * FROM test", params={}, orig="orig test"
        )

    monkeypatch.setattr(controller._model._session, "add", raise_dbapi_error)
    with pytest.raises(Exception) as exception_info:
        controller.put({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert (
        str(exception_info.value)
        == "A error occurred while querying database: (builtins.str) orig test\n[SQL: SELECT * FROM test]\n(Background on this error at: http://sqlalche.me/e/dbapi)"
    )


def test_exception_when_updating_one(controller: layabase.CRUDController, monkeypatch):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )

    def raise_exception(*args):
        raise Exception("This is the error message")

    monkeypatch.setattr(controller._model._session, "add", raise_exception)
    with pytest.raises(Exception) as exception_info:
        controller.put({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert str(exception_info.value) == "This is the error message"


def test_exception_when_deleting(controller: layabase.CRUDController, monkeypatch):
    def raise_exception(*args):
        raise Exception("This is the error message")

    monkeypatch.setattr(controller._model._session, "commit", raise_exception)
    with pytest.raises(Exception) as exception_info:
        controller.delete({})
    assert str(exception_info.value) == "This is the error message"


def test_post_with_empty_dict_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({})
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}


def test_post_without_providing_a_dictionary(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post("fail")
    assert exception_info.value.errors == {"_schema": ["Invalid input type."]}
    assert exception_info.value.received_data == None


def test_post_many_with_something_else_than_list_is_invalid(
    controller: layabase.CRUDController,
):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many("fail")
    assert exception_info.value.errors == {"": ["Must be a list of dictionaries."]}
    assert exception_info.value.received_data == "fail"


def test_put_without_providing_a_dictionary(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put("fail")
    assert exception_info.value.errors == {"": ["Must be a dictionary."]}
    assert exception_info.value.received_data == "fail"


def test_put_many_without_providing_a_list(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put_many("fail")
    assert exception_info.value.errors == {"": ["Must be a dictionary."]}
    assert exception_info.value.received_data == "f"


def test_post_many_with_something_else_than_list_of_dict_is_invalid(
    controller: layabase.CRUDController,
):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many(["fail"])
    assert exception_info.value.errors == {0: {"_schema": ["Invalid input type."]}}
    assert exception_info.value.received_data == [None]


def test_put_many_without_providing_a_list_of_dictionaries(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put_many(["fail"])
    assert exception_info.value.errors == {"": ["Must be a dictionary."]}
    assert exception_info.value.received_data == "fail"


def test_post_many_with_empty_list_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many([])
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}


def test_get_audit_when_not_audited(controller: layabase.CRUDController):
    assert controller.get_audit({}) == []


def test_put_with_nothing_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put(None)
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}


def test_put_with_empty_dict_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({})
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}


def test_delete_without_nothing_do_not_fail(controller: layabase.CRUDController):
    assert controller.delete({}) == 0
    assert controller.get_one({}) == {}


def test_post_without_mandatory_field_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"key": "my_key"})
    assert exception_info.value.errors == {
        "mandatory": ["Missing data for required field."]
    }
    assert exception_info.value.received_data == {"key": "my_key"}
    assert controller.get_one({}) == {}


def test_post_many_without_mandatory_field_is_invalid(
    controller: layabase.CRUDController,
):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many(
            [
                {"key": "my_key"},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ]
        )
    assert exception_info.value.errors == {
        0: {"mandatory": ["Missing data for required field."]}
    }
    assert exception_info.value.received_data == [
        {"key": "my_key"},
        {"key": "my_key", "mandatory": 1, "optional": "my_value"},
    ]
    assert controller.get_one({}) == {}


def test_post_without_key_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"mandatory": 1})
    assert exception_info.value.errors == {"key": ["Missing data for required field."]}
    assert exception_info.value.received_data == {"mandatory": 1}
    assert controller.get_one({}) == {}


def test_post_many_without_key_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many(
            [
                {"mandatory": 1},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ]
        )
    assert exception_info.value.errors == {
        0: {"key": ["Missing data for required field."]}
    }
    assert exception_info.value.received_data == [
        {"mandatory": 1},
        {"key": "my_key", "mandatory": 1, "optional": "my_value"},
    ]
    assert controller.get_one({}) == {}


def test_post_with_wrong_type_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"key": 256, "mandatory": 1})
    assert exception_info.value.errors == {"key": ["Not a valid string."]}
    assert exception_info.value.received_data == {"key": 256, "mandatory": 1}
    assert controller.get_one({}) == {}


def test_post_many_with_wrong_type_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many(
            [
                {"key": 256, "mandatory": 1},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ]
        )
    assert exception_info.value.errors == {0: {"key": ["Not a valid string."]}}
    assert exception_info.value.received_data == [
        {"key": 256, "mandatory": 1},
        {"key": "my_key", "mandatory": 1, "optional": "my_value"},
    ]
    assert controller.get_one({}) == {}


def test_put_with_wrong_type_is_invalid(controller: layabase.CRUDController):
    controller.post({"key": "value1", "mandatory": 1})
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({"key": "value1", "mandatory": "invalid value"})
    assert exception_info.value.errors == {"mandatory": ["Not a valid integer."]}
    assert exception_info.value.received_data == {
        "key": "value1",
        "mandatory": "invalid value",
    }


def test_post_without_optional_is_valid(controller: layabase.CRUDController):
    assert controller.post({"key": "my_key", "mandatory": 1}) == {
        "mandatory": 1,
        "key": "my_key",
        "optional": None,
    }
    assert controller.get_one({}) == {"key": "my_key", "mandatory": 1, "optional": None}


def test_post_many_without_optional_is_valid(controller: layabase.CRUDController):
    assert controller.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    ) == [
        {"mandatory": 1, "key": "my_key", "optional": None},
        {"mandatory": 2, "key": "my_key2", "optional": None},
    ]


def test_put_many_without_optional_is_valid(controller: layabase.CRUDController):
    controller.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )
    assert controller.put_many(
        [{"key": "my_key", "mandatory": 2}, {"key": "my_key2", "mandatory": 3}]
    ) == (
        [
            {"mandatory": 1, "key": "my_key", "optional": None},
            {"mandatory": 2, "key": "my_key2", "optional": None},
        ],
        [
            {"mandatory": 2, "key": "my_key", "optional": None},
            {"mandatory": 3, "key": "my_key2", "optional": None},
        ],
    )


def test_put_many_with_invalid_value(controller: layabase.CRUDController):
    controller.post_many(
        [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
    )
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put_many(
            [
                {"key": "my_key", "mandatory": "not integer"},
                {"key": "my_key2", "mandatory": 3},
            ]
        )
    assert exception_info.value.errors == {"mandatory": ["Not a valid integer."]}
    assert exception_info.value.received_data == {
        "key": "my_key",
        "mandatory": "not integer",
    }


def test_put_many_without_previous_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put_many(
            [{"key": "my_key", "mandatory": 2}, {"key": "my_key2", "mandatory": 3}]
        )
    assert exception_info.value.received_data == {"key": "my_key", "mandatory": 2}


def test_put_unexisting_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({"key": "my_key", "mandatory": 1, "optional": "my_value"})
    assert exception_info.value.received_data == {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
    }


def test_put_many_with_empty_list_is_invalid(controller: layabase.CRUDController):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put_many([])
    assert exception_info.value.errors == {"": ["No data provided."]}
    assert exception_info.value.received_data == {}


def test_get_no_like_operator(controller: layabase.CRUDController):
    controller.post_many(
        [
            {"key": "my_key", "mandatory": 1},
            {"key": "my_key2", "mandatory": 1},
            {"key": "my_ey", "mandatory": 1},
            {"key": "my_k", "mandatory": 1},
            {"key": "y_key", "mandatory": 1},
        ]
    )
    assert controller.get({"key": "*y_k*"}) == []


def test_get_one_with_a_list_of_one_value_is_valid(controller: layabase.CRUDController):
    controller.post({"key": "test", "mandatory": 1})
    controller.post({"key": "test2", "mandatory": 2})
    assert controller.get_one({"key": ["test2"]}) == {
        "key": "test2",
        "mandatory": 2,
        "optional": None,
    }


def test_get_one_with_an_empty_list_is_valid(controller: layabase.CRUDController):
    controller.post({"key": "test", "mandatory": 1})
    assert controller.get_one({"key": []}) == {
        "key": "test",
        "mandatory": 1,
        "optional": None,
    }


def test_get_one_with_a_list_of_values_is_invalid(controller: layabase.CRUDController):
    controller.post({"key": "test", "mandatory": 1})
    controller.post({"key": "test2", "mandatory": 2})
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.get_one({"key": ["test2", "test"]})
    assert exception_info.value.errors == {"key": ["Only one value must be queried."]}
    assert exception_info.value.received_data == {"key": ["test2", "test"]}


def test_post_with_optional_is_valid(controller: layabase.CRUDController):
    assert controller.post(
        {"key": "my_key", "mandatory": 1, "optional": "my_value"}
    ) == {"mandatory": 1, "key": "my_key", "optional": "my_value"}
    assert controller.get_one({}) == {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
    }


def test_post_many_with_optional_is_valid(controller: layabase.CRUDController):
    assert controller.post_many(
        [
            {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    ) == [
        {"mandatory": 1, "key": "my_key", "optional": "my_value"},
        {"mandatory": 2, "key": "my_key2", "optional": "my_value2"},
    ]


def test_post_with_unknown_field_is_valid(controller: layabase.CRUDController):
    assert (
        controller.post(
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            }
        )
        == {"mandatory": 1, "key": "my_key", "optional": "my_value"}
    )
    assert controller.get_one({}) == {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
    }


def test_post_many_with_unknown_field_is_valid(controller: layabase.CRUDController):
    assert controller.post_many(
        [
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                # This field do not exists in schema
                "unknown": "my_value",
            },
            {
                "key": "my_key2",
                "mandatory": 2,
                "optional": "my_value2",
                # This field do not exists in schema
                "unknown": "my_value2",
            },
        ]
    ) == [
        {"mandatory": 1, "key": "my_key", "optional": "my_value"},
        {"mandatory": 2, "key": "my_key2", "optional": "my_value2"},
    ]


def test_get_without_filter_is_retrieving_the_only_item(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller.get({}) == [
        {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
    ]


def test_get_one_without_filter_is_retrieving_the_only_item(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller.get_one({}) == {
        "mandatory": 1,
        "optional": "my_value1",
        "key": "my_key1",
    }


def test_get_from_another_thread_than_post(controller: layabase.CRUDController):
    def save_get_result():
        assert controller.get({}) == [
            {"mandatory": 1, "optional": "my_value1", "key": "my_key1"}
        ]

    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})

    get_thread = Thread(name="GetInOtherThread", target=save_get_result)
    get_thread.start()
    get_thread.join()


def test_get_without_filter_is_retrieving_everything_with_multiple_posts(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]


def test_get_without_filter_is_retrieving_everything(
    controller: layabase.CRUDController,
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


def test_get_with_filter_is_retrieving_subset_after_post_many(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert controller.get({"optional": "my_value1"}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ]


def test_get_with_filter_is_retrieving_subset_with_multiple_posts(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({"optional": "my_value1"}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ]


def test_get_with_list_filter_matching_one_is_retrieving_subset(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({"optional": ["my_value1"]}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ]


def test_get_with_list_filter_matching_many_is_retrieving_subset(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({"optional": ["my_value1", "my_value2"]}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]


def test_get_with_list_filter_matching_partial_is_retrieving_subset(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get(
        {"optional": ["non existing", "my_value1", "not existing"]}
    ) == [{"key": "my_key1", "mandatory": 1, "optional": "my_value1"}]


def test_get_with_empty_list_filter_is_retrieving_everything(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({"optional": []}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]


def test_delete_with_list_filter_matching_one_is_retrieving_subset(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.delete({"optional": ["my_value1"]}) == 1


def test_delete_with_list_filter_matching_many_is_retrieving_subset(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.delete({"optional": ["my_value1", "my_value2"]}) == 2


def test_delete_with_list_filter_matching_partial_is_retrieving_subset(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert (
        controller.delete({"optional": ["non existing", "my_value1", "not existing"]})
        == 1
    )


def test_delete_with_empty_list_filter_is_retrieving_everything(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.delete({"optional": []}) == 2


def test_get_with_filter_is_retrieving_subset(controller: layabase.CRUDController):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == controller.get({"optional": "my_value1"})


def test_put_is_updating_and_get_retrieval(controller: layabase.CRUDController):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller.put({"key": "my_key1", "optional": "my_value"}) == (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    )
    assert controller.get({"mandatory": 1}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ]


def test_put_is_updating_and_get_one_retrieval(controller: layabase.CRUDController):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert controller.put({"key": "my_key1", "optional": "my_value"}) == (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    )
    assert controller.get_one({"mandatory": 1}) == {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value",
    }


def test_history_retrieve_all(controller: layabase.CRUDController):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "optional": "my_value"})
    assert controller.get_history({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ]


def test_get_last_returns_latest(controller: layabase.CRUDController):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "optional": "my_value"})
    assert controller.get_last({}) == {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value",
    }


def test_rollback_does_nothing(controller: layabase.CRUDController):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "optional": "my_value"})
    assert controller.rollback_to({"revision": 0}) == 0
    assert controller.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"}
    ]


def test_put_is_updating_and_previous_value_cannot_be_used_to_filter_on_get(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "optional": "my_value"})
    assert controller.get({"optional": "my_value1"}) == []


def test_puttest_put_is_updating_is_updating_and_previous_value_cannot_be_used_to_filter_on_get_one(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.put({"key": "my_key1", "optional": "my_value"})
    assert controller.get_one({"optional": "my_value1"}) == {}


def test_post_many_get_one_with_filter_is_retrieving_the_proper_row(
    controller: layabase.CRUDController,
):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert controller.get_one({"optional": "my_value1"}) == {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value1",
    }


def test_get_one_with_filter_is_retrieving_the_proper_row_after_multiple_posts(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get_one({"optional": "my_value1"}) == {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value1",
    }


def test_delete_with_filter_is_removing_the_proper_row(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.delete({"key": "my_key1"}) == 1
    assert controller.get({}) == [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ]


def test_delete_without_filter_is_removing_everything(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.delete({}) == 2
    assert controller.get({}) == []


def test_get_with_order_by_desc_is_retrieving_elements_ordered_by_descending_mode(
    controller,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    controller.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert controller.get({"order_by": ["key desc"]}) == [
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
    ]


def test_get_with_order_by_is_retrieving_elements_ordered_by_ascending_mode(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({"order_by": ["key"]}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ]


def test_get_with_2_order_by_is_retrieving_elements_ordered_by(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert controller.get({"order_by": ["key", "mandatory desc"]}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ]


def test_get_order_by_sqla_columns(controller: layabase.CRUDController):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 1, "optional": "my_value2"},
            {"key": "my_key3", "mandatory": -1, "optional": "my_value3"},
        ]
    )
    assert controller.get(
        {
            "order_by": [
                sqlalchemy.asc(controller._model.mandatory),
                sqlalchemy.desc(controller._model.key),
            ]
        }
    ) == [
        {"key": "my_key3", "mandatory": -1, "optional": "my_value3"},
        {"key": "my_key2", "mandatory": 1, "optional": "my_value2"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
    ]


def test_get_without_filter_is_failing_if_more_than_one_item_exists(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.get_one({})
    assert exception_info.value.errors == {
        "": ["More than one result: Consider another filtering."]
    }
    assert exception_info.value.received_data == {}


def test_get_with_limit_2_is_retrieving_subset_of_2_first_elements(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    controller.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert controller.get({"limit": 2}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ]


def test_get_with_offset_1_is_retrieving_subset_of_n_minus_1_first_elements(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    controller.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert controller.get({"offset": 1}) == [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ]


def test_get_with_limit_1_and_offset_1_is_retrieving_middle_element(
    controller: layabase.CRUDController,
):
    controller.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    controller.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    controller.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    assert controller.get({"offset": 1, "limit": 1}) == [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ]


def test_get_model_description_returns_description(controller: layabase.CRUDController):
    assert controller.get_model_description() == {
        "key": "key",
        "mandatory": "mandatory",
        "optional": "optional",
        "table": "test",
    }
