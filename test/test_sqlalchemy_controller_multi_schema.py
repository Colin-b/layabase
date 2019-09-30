import pytest
import sqlalchemy

import layabase
import layabase.database_sqlalchemy


def test_multi_schema_not_handled():
    class TestModel:
        __tablename__ = "test"
        __table_args__ = {u"schema": "schema_name1"}

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    class TestModel2:
        __tablename__ = "test"
        __table_args__ = {u"schema": "schema_name2"}

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    with pytest.raises(
        layabase.database_sqlalchemy.MultiSchemaNotSupported
    ) as exception_info:
        layabase.load(
            "sqlite:///:memory:",
            [layabase.CRUDController(TestModel), layabase.CRUDController(TestModel2)],
        )
    assert str(exception_info.value) == "SQLite does not manage multi-schemas.."
