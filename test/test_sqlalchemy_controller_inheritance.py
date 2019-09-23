import pytest
import sqlalchemy

from layabase import database, database_sqlalchemy
from test.flask_restplus_mock import TestAPI


class TestInheritanceController(database.CRUDController):
    pass


def _create_models(base):
    class Inherited:
        optional = sqlalchemy.Column(sqlalchemy.String)

    class TestInheritanceModel(database_sqlalchemy.CRUDModel, Inherited, base):
        __tablename__ = "inheritance_table_name"

        key = sqlalchemy.Column(
            sqlalchemy.Integer, primary_key=True, autoincrement=True
        )
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)

    TestInheritanceModel.audit()

    TestInheritanceController.model(TestInheritanceModel)
    return [TestInheritanceModel]


@pytest.fixture
def db():
    _db = database.load("sqlite:///:memory:", _create_models)
    TestInheritanceController.namespace(TestAPI)
    yield _db
    database.reset(_db)


def test_inheritance_does_not_throw_errors(db):
    pass
