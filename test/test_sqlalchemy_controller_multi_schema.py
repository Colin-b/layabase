import pytest
import sqlalchemy

import layabase


def test_multi_schema_not_handled():
    class TestTable:
        __tablename__ = "test"
        __table_args__ = {u"schema": "schema_name1"}

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    class TestTable2:
        __tablename__ = "test"
        __table_args__ = {u"schema": "schema_name2"}

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    with pytest.raises(layabase.MultiSchemaNotSupported) as exception_info:
        layabase.load(
            "sqlite:///:memory:",
            [layabase.CRUDController(TestTable), layabase.CRUDController(TestTable2)],
        )
    assert str(exception_info.value) == "SQLite does not manage multi-schemas.."
