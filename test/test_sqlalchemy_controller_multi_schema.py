import pytest
import sqlalchemy

import layabase
import layabase.database_sqlalchemy


def test_multi_schema_not_handled():
    class TestController(layabase.CRUDController):
        class TestModel:
            __tablename__ = "sample_table_name"
            __table_args__ = {u"schema": "schema_name1"}

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

        model = TestModel

    class TestController2(layabase.CRUDController):
        class TestModel2:
            __tablename__ = "sample_table_name"
            __table_args__ = {u"schema": "schema_name2"}

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

        model = TestModel2

    with pytest.raises(
        layabase.database_sqlalchemy.MultiSchemaNotSupported
    ) as exception_info:
        layabase.load("sqlite:///:memory:", [TestController, TestController2])
    assert str(exception_info.value) == "SQLite does not manage multi-schemas.."
