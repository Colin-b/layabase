import pytest

import layabase
import layabase._database_mongo


def test_namespace_method_without_connecting_to_database():
    class TestNamespace:
        pass

    class TestCollection:
        __collection_name__ = "test"

        id = layabase._database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestCollection).namespace(TestNamespace)
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_counters_table_name_is_forbidden():
    class TestCollection:
        __collection_name__ = "counters"

        key = layabase._database_mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        layabase.load("mongomock", [layabase.CRUDController(TestCollection)])

    assert "counters is a reserved collection name." == str(exception_info.value)


def test_audit_table_name_is_forbidden():
    class TestCollection:
        __collection_name__ = "audit"

        key = layabase._database_mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        layabase.load("mongomock", [layabase.CRUDController(TestCollection)])

    assert "audit is a reserved collection name." == str(exception_info.value)


def test_audit_prefixed_table_name_is_forbidden():
    class TestCollection:
        __collection_name__ = "audit_toto"

        key = layabase._database_mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        layabase.load("mongomock", [layabase.CRUDController(TestCollection)])

    assert "audit_toto is a reserved collection name." == str(exception_info.value)


def test_get_method_without_connecting_to_database():
    class TestCollection:
        __collection_name__ = "test"

        id = layabase._database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestCollection).get({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_post_method_without_connecting_to_database():
    class TestCollection:
        __collection_name__ = "test"

        id = layabase._database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestCollection).post({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_post_many_method_without_connecting_to_database():
    class TestCollection:
        __collection_name__ = "test"

        id = layabase._database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestCollection).post_many([])
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_put_method_without_connecting_to_database():
    class TestCollection:
        __collection_name__ = "test"

        id = layabase._database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestCollection).put({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_delete_method_without_connecting_to_database():
    class TestCollection:
        __collection_name__ = "test"

        id = layabase._database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestCollection).delete({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_audit_method_without_connecting_to_database():
    class TestCollection:
        __collection_name__ = "test"

        id = layabase._database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestCollection).get_audit({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_model_description_method_without_connecting_to_database():
    class TestCollection:
        __collection_name__ = "test"

        id = layabase._database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestCollection).get_model_description()
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )
