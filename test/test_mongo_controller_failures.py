import pytest

import layabase
import layabase.database_mongo


def test_load_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.load("mongomock", [TestController])
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_namespace_method_without_setting_model():
    class TestNamespace:
        pass

    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.namespace(TestNamespace)
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_counters_table_name_is_forbidden():
    class TestController(layabase.CRUDController):
        class TestModel:
            __tablename__ = "counters"

            key = layabase.database_mongo.Column(str)

        model = TestModel

    with pytest.raises(Exception) as exception_info:
        layabase.load("mongomock", [TestController])

    assert "counters is a reserved collection name." == str(exception_info.value)


def test_audit_table_name_is_forbidden():
    class TestController(layabase.CRUDController):
        class TestModel:
            __tablename__ = "audit"

            key = layabase.database_mongo.Column(str)

        model = TestModel

    with pytest.raises(Exception) as exception_info:
        layabase.load("mongomock", [TestController])

    assert "audit is a reserved collection name." == str(exception_info.value)


def test_audit_prefixed_table_name_is_forbidden():
    class TestController(layabase.CRUDController):
        class TestModel:
            __tablename__ = "audit_toto"

            key = layabase.database_mongo.Column(str)

        model = TestModel

    with pytest.raises(Exception) as exception_info:
        layabase.load("mongomock", [TestController])

    assert "audit_toto is a reserved collection name." == str(exception_info.value)


def test_get_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.get({})
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_post_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.post({})
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_post_many_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.post_many([])
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_put_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.put({})
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_delete_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.delete({})
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_audit_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.get_audit({})
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_model_description_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.get_model_description()
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )
