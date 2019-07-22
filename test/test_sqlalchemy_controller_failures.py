import re

import pytest

from pycommon_database import database


class TestController(database.CRUDController):
    pass


def test_model_method_without_setting_model():
    with pytest.raises(Exception) as exception_info:
        TestController.model(None)
    assert re.match(
        "Model was not attached to TestController. "
        "Call <bound method CRUDController.model of <class '.*TestController'>>.",
        str(exception_info.value),
    )


def test_namespace_method_without_setting_model():
    class TestNamespace:
        pass

    with pytest.raises(Exception) as exception_info:
        TestController.namespace(TestNamespace)
    assert re.match(
        "Model was not attached to TestController. "
        "Call <bound method CRUDController.model of <class '.*TestController'>>.",
        str(exception_info.value),
    )


def test_get_method_without_setting_model():
    with pytest.raises(Exception) as exception_info:
        TestController.get({})
    assert re.match(
        "Model was not attached to TestController. "
        "Call <bound method CRUDController.model of <class '.*TestController'>>.",
        str(exception_info.value),
    )


def test_post_method_without_setting_model():
    with pytest.raises(Exception) as exception_info:
        TestController.post({})
    assert re.match(
        "Model was not attached to TestController. "
        "Call <bound method CRUDController.model of <class '.*TestController'>>.",
        str(exception_info.value),
    )


def test_post_many_method_without_setting_model():
    with pytest.raises(Exception) as exception_info:
        TestController.post_many([])
    assert re.match(
        "Model was not attached to TestController. "
        "Call <bound method CRUDController.model of <class '.*TestController'>>.",
        str(exception_info.value),
    )


def test_put_method_without_setting_model():
    with pytest.raises(Exception) as exception_info:
        TestController.put({})
    assert re.match(
        "Model was not attached to TestController. "
        "Call <bound method CRUDController.model of <class '.*TestController'>>.",
        str(exception_info.value),
    )


def test_delete_method_without_setting_model():
    with pytest.raises(Exception) as exception_info:
        TestController.delete({})
    assert re.match(
        "Model was not attached to TestController. "
        "Call <bound method CRUDController.model of <class '.*TestController'>>.",
        str(exception_info.value),
    )


def test_audit_method_without_setting_model():
    with pytest.raises(Exception) as exception_info:
        TestController.get_audit({})
    assert re.match(
        "Model was not attached to TestController. "
        "Call <bound method CRUDController.model of <class '.*TestController'>>.",
        str(exception_info.value),
    )


def test_model_description_method_without_setting_model():
    with pytest.raises(Exception) as exception_info:
        TestController.get_model_description()
    assert re.match(
        "Model was not attached to TestController. "
        "Call <bound method CRUDController.model of <class '.*TestController'>>.",
        str(exception_info.value),
    )
