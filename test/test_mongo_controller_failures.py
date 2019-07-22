import re

import pytest

from pycommon_database import database, database_mongo


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


def test_counters_table_name_is_forbidden():
    def create_models(base):
        class TestModel(database_mongo.CRUDModel, base=base, table_name="counters"):
            key = database_mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        database.load("mongomock", create_models)

    assert "counters is a reserved collection name." == str(exception_info.value)


def test_audit_table_name_is_forbidden():
    def create_models(base):
        class TestModel(database_mongo.CRUDModel, base=base, table_name="audit"):
            key = database_mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        database.load("mongomock", create_models)

    assert "audit is a reserved collection name." == str(exception_info.value)


def test_audit_prefixed_table_name_is_forbidden():
    def create_models(base):
        class TestModel(database_mongo.CRUDModel, base=base, table_name="audit_toto"):
            key = database_mongo.Column(str)

    with pytest.raises(Exception) as exception_info:
        database.load("mongomock", create_models)

    assert "audit_toto is a reserved collection name." == str(exception_info.value)


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
