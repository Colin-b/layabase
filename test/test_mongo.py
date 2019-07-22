import pymongo
import pytest

from pycommon_database import database, database_mongo, versioning_mongo


def test_str_column_cannot_auto_increment():
    with pytest.raises(Exception) as exception_info:
        database_mongo.Column(should_auto_increment=True)
    assert str(exception_info.value) == "Only int fields can be auto incremented."


def test_auto_incremented_field_cannot_be_non_nullable():
    with pytest.raises(Exception) as exception_info:
        database_mongo.Column(int, should_auto_increment=True, is_nullable=False)
    assert (
        str(exception_info.value)
        == "A field cannot be mandatory and auto incremented at the same time."
    )


def test_field_with_default_value_cannot_be_non_nullable():
    with pytest.raises(Exception) as exception_info:
        database_mongo.Column(default_value="test", is_nullable=False)
    assert (
        str(exception_info.value)
        == "A field cannot be mandatory and having a default value at the same time."
    )


def test_spaces_are_forbidden_at_start_of_column_name():
    with pytest.raises(Exception) as exception_info:
        database_mongo.Column(name="   test")
    assert (
        str(exception_info.value)
        == "   test is not a valid name. Spaces are not allowed at start or end of field names."
    )


def test_spaces_are_forbidden_at_end_of_column_name():
    with pytest.raises(Exception) as exception_info:
        database_mongo.Column(name="test   ")
    assert (
        str(exception_info.value)
        == "test    is not a valid name. Spaces are not allowed at start or end of field names."
    )


def test_spaces_are_forbidden_at_start_and_end_of_column_name():
    with pytest.raises(Exception) as exception_info:
        database_mongo.Column(name="   test   ")
    assert (
        str(exception_info.value)
        == "   test    is not a valid name. Spaces are not allowed at start or end of field names."
    )


def test_none_connection_string_is_invalid():
    with pytest.raises(Exception) as exception_info:
        database.load(None, None)
    assert str(exception_info.value) == "A database connection URL must be provided."


def test_empty_connection_string_is_invalid():
    with pytest.raises(Exception) as exception_info:
        database.load("", None)
    assert str(exception_info.value) == "A database connection URL must be provided."


def test_no_create_models_function_is_invalid():
    with pytest.raises(Exception) as exception_info:
        database.load("mongomock", None)
    assert (
        str(exception_info.value)
        == "A method allowing to create related models must be provided."
    )


@pytest.fixture
def db():
    _db = database.load("mongomock", lambda x: x)
    yield _db
    database.reset(_db)


def test_2entities_on_same_collection_without_pk(db):
    class TestEntitySameCollection1(
        versioning_mongo.VersionedCRUDModel,
        base=db,
        table_name="sample_table_name_2entities",
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        mandatory = database_mongo.Column(int, is_nullable=False)
        optional = database_mongo.Column(str)

    TestEntitySameCollection1.add({"key": "1", "mandatory": 2})
    TestEntitySameCollection1.add({"key": "2", "mandatory": 2})

    with pytest.raises(pymongo.errors.DuplicateKeyError):

        class TestEntitySameCollection2(
            versioning_mongo.VersionedCRUDModel,
            base=db,
            table_name="sample_table_name_2entities",
        ):
            pass


def test_2entities_on_same_collection_with_pk(db):
    class TestEntitySameCollection1(
        versioning_mongo.VersionedCRUDModel,
        base=db,
        table_name="sample_table_name_2entities",
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        mandatory = database_mongo.Column(int, is_nullable=False)
        optional = database_mongo.Column(str)

    TestEntitySameCollection1.add({"key": "1", "mandatory": 2})
    TestEntitySameCollection1.add({"key": "2", "mandatory": 2})

    class TestEntitySameCollection2(
        versioning_mongo.VersionedCRUDModel,
        base=db,
        table_name="sample_table_name_2entities",
        skip_update_indexes=True,
    ):
        pass

    TestEntitySameCollection1.add({"key": "3", "mandatory": 2})
    assert len(TestEntitySameCollection1.get_all()) == 3
