import pytest

from layabase import database


def test_none_connection_string_is_invalid():
    with pytest.raises(Exception) as exception_info:
        database.load(None, None)
    assert "A database connection URL must be provided." == str(exception_info.value)


def test_empty_connection_string_is_invalid():
    with pytest.raises(Exception) as exception_info:
        database.load("", None)
    assert "A database connection URL must be provided." == str(exception_info.value)


def test_no_create_models_function_is_invalid():
    with pytest.raises(Exception) as exception_info:
        database.load("sqlite:///:memory:", None)
    assert "A method allowing to create related models must be provided." == str(
        exception_info.value
    )
