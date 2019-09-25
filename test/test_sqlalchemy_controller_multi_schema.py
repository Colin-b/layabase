import pytest
import sqlalchemy

from layabase import database, database_sqlalchemy


def _create_models(base):
    class TestModel(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "sample_table_name"
        __table_args__ = {u"schema": "schema_name1"}

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    class TestModel2(database_sqlalchemy.CRUDModel, base):
        __tablename__ = "sample_table_name"
        __table_args__ = {u"schema": "schema_name2"}

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    return [TestModel, TestModel2]


def test_multi_schema_not_handled():
    with pytest.raises(database_sqlalchemy.MultiSchemaNotSupported) as exception_info:
        database.load("sqlite:///:memory:", _create_models)
    assert str(exception_info.value) == "SQLite does not manage multi-schemas.."
