import pytest

import layabase
import layabase.database_mongo


def test_namespace_method_without_setting_model():
    class TestNamespace:
        pass

    class TestModel:
        __tablename__ = "test"

        id = layabase.database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).namespace(TestNamespace)
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_counters_table_name_is_forbidden():
    class TestModel:
        __tablename__ = "counters"

        key = layabase.database_mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        layabase.load("mongomock", [layabase.CRUDController(TestModel)])

    assert "counters is a reserved collection name." == str(exception_info.value)


def test_audit_table_name_is_forbidden():
    class TestModel:
        __tablename__ = "audit"

        key = layabase.database_mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        layabase.load("mongomock", [layabase.CRUDController(TestModel)])

    assert "audit is a reserved collection name." == str(exception_info.value)


def test_audit_prefixed_table_name_is_forbidden():
    class TestModel:
        __tablename__ = "audit_toto"

        key = layabase.database_mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        layabase.load("mongomock", [layabase.CRUDController(TestModel)])

    assert "audit_toto is a reserved collection name." == str(exception_info.value)


def test_get_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = layabase.database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).get({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_post_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = layabase.database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).post({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_post_many_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = layabase.database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).post_many([])
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_put_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = layabase.database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).put({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_delete_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = layabase.database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).delete({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_audit_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = layabase.database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).get_audit({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_model_description_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = layabase.database_mongo.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).get_model_description()
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )
