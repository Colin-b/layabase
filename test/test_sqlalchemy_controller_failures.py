import pytest
import sqlalchemy

import layabase


def test_namespace_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    class TestNamespace:
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).namespace(TestNamespace)
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).get({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_one_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).get_one({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_last_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).get_last({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_history_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).get_history({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_field_names_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).get_field_names()
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_url_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).get_url("test")
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_post_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).post({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_post_many_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).post_many([])
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_put_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).put({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_put_many_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).put_many({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_delete_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).delete({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_rollback_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).rollback_to({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_audit_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).get_audit({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_model_description_method_without_setting_model():
    class TestModel:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestModel).get_model_description()
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )
