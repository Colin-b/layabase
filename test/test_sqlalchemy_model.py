import pytest
import sqlalchemy
from layaberr import ValidationFailed, ModelCouldNotBeFound


from layabase import database, database_sqlalchemy
import layabase.testing
from test import DateTimeModuleMock


class SaveModel:
    pass


def _create_models(base):
    class TestModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "sample_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    class TestModelAutoIncr(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "autoincre_table_name"

        key = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.String, nullable=False)

    SaveModel._model = TestModel
    return [TestModel, TestModelAutoIncr]


@pytest.fixture
def db():
    _db = database.load("sqlite:///:memory:", _create_models)
    yield _db
    layabase.testing.reset(_db)


def test_health_details(db, monkeypatch):
    monkeypatch.setattr(database_sqlalchemy, "datetime", DateTimeModuleMock)
    health_status = database.check(db)
    expected_result = (
        "pass",
        {
            "sqlite:select": {
                "componentType": "datastore",
                "observedValue": "",
                "status": "pass",
                "time": "2018-10-11T15:05:05.663979",
            }
        },
    )
    assert expected_result == health_status


def test_health_details_no_db(db):
    with pytest.raises(Exception) as exception_info:
        database.check(None)
    assert "A database connection URL must be provided." == str(exception_info.value)


def test_get_all_without_data_returns_empty_list(db):
    assert [] == SaveModel._model.get_all()


def test_get_without_data_returns_empty_dict(db):
    assert {} == SaveModel._model.get()


def test_add_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.add(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_primary_keys_are_returned(db):
    assert ["key", "mandatory"] == SaveModel._model.get_primary_keys()


def test_add_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.add({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_update_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.update(None)
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_update_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.update({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data
    assert {} == SaveModel._model.get()


def test_remove_without_nothing_do_not_fail(db):
    assert 0 == SaveModel._model.remove()
    assert {} == SaveModel._model.get()


def test_add_without_mandatory_field_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.add({"key": "my_key"})
    assert {
        "mandatory": ["Missing data for required field."]
    } == exception_info.value.errors
    assert {"key": "my_key"} == exception_info.value.received_data
    assert {} == SaveModel._model.get()


def test_add_without_key_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.add({"mandatory": 1})
    assert {"key": ["Missing data for required field."]} == exception_info.value.errors
    assert {"mandatory": 1} == exception_info.value.received_data
    assert {} == SaveModel._model.get()


def test_add_with_wrong_type_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.add({"key": 256, "mandatory": 1})
    assert {"key": ["Not a valid string."]} == exception_info.value.errors
    assert {"key": 256, "mandatory": 1} == exception_info.value.received_data
    assert {} == SaveModel._model.get()


def test_update_with_wrong_type_is_invalid(db):
    SaveModel._model.add({"key": "value1", "mandatory": 1})
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.update({"key": "value1", "mandatory": "invalid_value"})
    assert {"mandatory": ["Not a valid integer."]} == exception_info.value.errors
    assert {
        "key": "value1",
        "mandatory": "invalid_value",
    } == exception_info.value.received_data


def test_add_all_with_nothing_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.add_all(None)
    assert {"": ["No data provided."]} == exception_info.value.errors


def test_add_all_with_empty_dict_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.add_all({})
    assert {"": ["No data provided."]} == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_add_all_without_mandatory_field_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.add_all(
            [
                {"key": "my_key"},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ]
        )
    assert {
        0: {"mandatory": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [
        {"key": "my_key"},
        {"key": "my_key", "mandatory": 1, "optional": "my_value"},
    ] == exception_info.value.received_data
    assert {} == SaveModel._model.get()


def test_add_all_without_key_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.add_all(
            [
                {"mandatory": 1},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ]
        )
    assert {
        0: {"key": ["Missing data for required field."]}
    } == exception_info.value.errors
    assert [
        {"mandatory": 1},
        {"key": "my_key", "mandatory": 1, "optional": "my_value"},
    ] == exception_info.value.received_data
    assert {} == SaveModel._model.get()


def test_add_all_with_wrong_type_is_invalid(db):
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.add_all(
            [
                {"key": 256, "mandatory": 1},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ]
        )
    assert {0: {"key": ["Not a valid string."]}} == exception_info.value.errors
    assert [
        {"key": 256, "mandatory": 1},
        {"key": "my_key", "mandatory": 1, "optional": "my_value"},
    ] == exception_info.value.received_data
    assert {} == SaveModel._model.get()


def test_add_without_optional_is_valid(db):
    assert {"mandatory": 1, "key": "my_key", "optional": None} == SaveModel._model.add(
        {"key": "my_key", "mandatory": 1}
    )
    assert {"key": "my_key", "mandatory": 1, "optional": None} == SaveModel._model.get()


def test_add_with_optional_is_valid(db):
    assert {
        "mandatory": 1,
        "key": "my_key",
        "optional": "my_value",
    } == SaveModel._model.add({"key": "my_key", "mandatory": 1, "optional": "my_value"})
    assert {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
    } == SaveModel._model.get()


def test_update_unexisting_is_invalid(db):
    with pytest.raises(ModelCouldNotBeFound) as exception_info:
        SaveModel._model.update(
            {"key": "my_key", "mandatory": 1, "optional": "my_value"}
        )
    assert {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
    } == exception_info.value.requested_data


def test_add_with_unknown_field_is_valid(db):
    assert {
        "mandatory": 1,
        "key": "my_key",
        "optional": "my_value",
    } == SaveModel._model.add(
        {
            "key": "my_key",
            "mandatory": 1,
            "optional": "my_value",
            # This field do not exists in schema
            "unknown": "my_value",
        }
    )
    assert {
        "key": "my_key",
        "mandatory": 1,
        "optional": "my_value",
    } == SaveModel._model.get()


def test_get_without_filter_is_retrieving_the_only_item(db):
    SaveModel._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert {
        "mandatory": 1,
        "optional": "my_value1",
        "key": "my_key1",
    } == SaveModel._model.get()


def test_get_without_filter_is_failing_if_more_than_one_item_exists(db):
    SaveModel._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    SaveModel._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    with pytest.raises(ValidationFailed) as exception_info:
        SaveModel._model.get()
    assert {
        "": ["More than one result: Consider another filtering."]
    } == exception_info.value.errors
    assert {} == exception_info.value.received_data


def test_get_all_without_filter_is_retrieving_everything_after_multiple_posts(db):
    SaveModel._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    SaveModel._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == SaveModel._model.get_all()


def test_get_all_without_filter_is_retrieving_everything(db):
    SaveModel._model.add_all(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == SaveModel._model.get_all()


def test_get_all_with_filter_is_retrieving_subset_after_multiple_posts(db):
    SaveModel._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    SaveModel._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == SaveModel._model.get_all(optional="my_value1")


def test_get_all_with_filter_is_retrieving_subset(db):
    SaveModel._model.add_all(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    ] == SaveModel._model.get_all(optional="my_value1")


def test_get_all_order_by(db):
    SaveModel._model.add_all(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 1, "optional": "my_value2"},
            {"key": "my_key3", "mandatory": -1, "optional": "my_value3"},
        ]
    )
    assert [
        {"key": "my_key3", "mandatory": -1, "optional": "my_value3"},
        {"key": "my_key2", "mandatory": 1, "optional": "my_value2"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
    ] == SaveModel._model.get_all(
        order_by=[
            sqlalchemy.asc(SaveModel._model.mandatory),
            sqlalchemy.desc(SaveModel._model.key),
        ]
    )


def test_get_with_filter_is_retrieving_the_proper_row_after_multiple_posts(db):
    SaveModel._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    SaveModel._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value1",
    } == SaveModel._model.get(optional="my_value1")


def test_get_with_filter_is_retrieving_the_proper_row(db):
    SaveModel._model.add_all(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        ]
    )
    assert {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value1",
    } == SaveModel._model.get(optional="my_value1")


def test_update_is_updating(db):
    SaveModel._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    assert (
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
    ) == SaveModel._model.update({"key": "my_key1", "optional": "my_value"})
    assert {
        "key": "my_key1",
        "mandatory": 1,
        "optional": "my_value",
    } == SaveModel._model.get(mandatory=1)


def test_update_is_updating_and_previous_value_cannot_be_used_to_filter(db):
    SaveModel._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    SaveModel._model.update({"key": "my_key1", "optional": "my_value"})
    assert {} == SaveModel._model.get(optional="my_value1")


def test_remove_with_filter_is_removing_the_proper_row(db):
    SaveModel._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    SaveModel._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 1 == SaveModel._model.remove(key="my_key1")
    assert [
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    ] == SaveModel._model.get_all()


def test_remove_without_filter_is_removing_everything(db):
    SaveModel._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    SaveModel._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    assert 2 == SaveModel._model.remove()
    assert [] == SaveModel._model.get_all()
