import pytest
import sqlalchemy

import layabase


def test_namespace_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    class TestNamespace:
        pass

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).namespace(TestNamespace)
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).get({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_one_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).get_one({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_last_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).get_last({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_history_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).get_history({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_field_names_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).get_field_names()
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_get_url_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).get_url("test")
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_post_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).post({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_post_many_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).post_many([])
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_put_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).put({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_put_many_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).put_many({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_delete_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).delete({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_rollback_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).rollback_to({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_audit_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).get_audit({})
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )


def test_model_description_method_without_connecting_to_database():
    class TestTable:
        __tablename__ = "test"

        id = sqlalchemy.Column()

    with pytest.raises(layabase.ControllerModelNotSet) as exception_info:
        layabase.CRUDController(TestTable).get_model_description()
    assert (
        str(exception_info.value)
        == "layabase.load must be called with this CRUDController instance before using any provided CRUDController feature."
    )
