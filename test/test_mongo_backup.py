import tempfile

import pytest
import flask
import flask_restplus

from layabase import database, database_mongo


class TestController(database.CRUDController):
    pass


class TestSecondController(database.CRUDController):
    pass


def _create_models(base):
    class TestModel(
        database_mongo.CRUDModel, base=base, table_name="sample_table_name"
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        mandatory = database_mongo.Column(int, is_nullable=False)
        optional = database_mongo.Column(str)

    class TestModelSecond(
        database_mongo.CRUDModel, base=base, table_name="second_sample_table_name"
    ):
        key = database_mongo.Column(str, is_primary_key=True)
        mandatory = database_mongo.Column(int, is_nullable=False)
        optional = database_mongo.Column(str)

    TestController.model(TestModel)
    TestSecondController.model(TestModelSecond)
    return [TestModel, TestModelSecond]


@pytest.fixture
def app():
    application = flask.Flask(__name__)
    application.testing = True
    return application


@pytest.fixture
def api(app):
    return flask_restplus.Api(app)


@pytest.fixture
def db(api):
    _db = database.load("mongomock", _create_models)
    TestController.namespace(api)
    TestSecondController.namespace(api)

    yield _db

    database.reset(_db)


def test_dump_delete_restore_is_restoring_db_dumped(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    TestSecondController.post(
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    )
    TestSecondController.post(
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    )

    with tempfile.TemporaryDirectory() as temp_directory:
        database.dump(db, temp_directory)
        TestController.delete({"key": "my_key1"})
        TestSecondController.delete({"key": "my_key1"})
        database.restore(db, temp_directory)

    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ] == TestController.get({})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestSecondController.get({})


def test_dump_delete_all_restore_is_restoring_db_dumped(db):
    TestController.post({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
    TestController.post({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
    TestController.post({"key": "my_key3", "mandatory": 3, "optional": "my_value3"})
    TestSecondController.post(
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
    )
    TestSecondController.post(
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
    )

    with tempfile.TemporaryDirectory() as temp_directory:
        database.dump(db, temp_directory)
        TestController.delete({})
        TestSecondController.delete({})
        database.restore(db, temp_directory)

    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
        {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
    ] == TestController.get({})
    assert [
        {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
        {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
    ] == TestSecondController.get({})
