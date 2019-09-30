import pytest

import layabase


def test_controller_without_setting_model():
    with pytest.raises(Exception) as exception_info:
        layabase.CRUDController(None)
    assert str(exception_info.value) == "Model must be provided."


def test_none_connection_string_is_invalid():
    with pytest.raises(layabase.NoDatabaseProvided) as exception_info:
        layabase.load(None, None)
    assert str(exception_info.value) == "A database connection URL must be provided."


def test_empty_connection_string_is_invalid():
    with pytest.raises(layabase.NoDatabaseProvided) as exception_info:
        layabase.load("", None)
    assert str(exception_info.value) == "A database connection URL must be provided."


def test_no_controllers_is_invalid():
    with pytest.raises(layabase.NoRelatedControllers) as exception_info:
        layabase.load("sqlite:///:memory:", None)
    assert str(exception_info.value) == "A list of CRUDController must be provided."
