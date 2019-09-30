import pytest

import layabase


def test_load_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.load("sqlite:///:memory:", [TestController])
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_namespace_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    class TestNamespace:
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.namespace(TestNamespace)
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_get_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.get({})
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_get_one_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.get_one({})
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_get_last_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.get_last({})
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_get_history_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.get_history({})
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_get_field_names_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.get_field_names()
    assert (
        str(exception_info.value)
        == "TestController.model must be set before calling layabase.load function."
    )


def test_get_url_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.get_url("test")
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


def test_put_many_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.put_many({})
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


def test_rollback_method_without_setting_model():
    class TestController(layabase.CRUDController):
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        TestController.rollback_to({})
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
