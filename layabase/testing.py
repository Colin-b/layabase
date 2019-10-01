import datetime

import pytest


def reset(base):
    """
    If the database was already created, then drop all tables and recreate them all.

    :param base: database object as returned by the load function (Mandatory).
    """
    if hasattr(base, "is_mongos"):
        import layabase.database_mongo as database_mongo

        database_mongo._reset(base)
    else:
        import layabase.database_sqlalchemy as database_sqlalchemy

        database_sqlalchemy._reset(base)


_date_time_for_tests = datetime.datetime(2018, 10, 11, 15, 5, 5, 663979)


class DateTimeModuleMock:
    class DateTimeMock:
        @staticmethod
        def utcnow():
            return _date_time_for_tests

    datetime = DateTimeMock


@pytest.fixture
def mock_mongo_health_datetime(monkeypatch):
    import layabase.database_mongo

    monkeypatch.setattr(layabase.database_mongo, "datetime", DateTimeModuleMock)


@pytest.fixture
def mock_mongo_audit_datetime(monkeypatch):
    import layabase.audit_mongo

    monkeypatch.setattr(layabase.audit_mongo, "datetime", DateTimeModuleMock)


@pytest.fixture
def mock_sqlalchemy_health_datetime(monkeypatch):
    import layabase.database_sqlalchemy

    monkeypatch.setattr(layabase.database_sqlalchemy, "datetime", DateTimeModuleMock)


@pytest.fixture
def mock_sqlalchemy_audit_datetime(monkeypatch):
    import layabase.audit_sqlalchemy

    monkeypatch.setattr(layabase.audit_sqlalchemy, "datetime", DateTimeModuleMock)
