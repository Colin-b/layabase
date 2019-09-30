import pytest
import sqlalchemy

import layabase
import layabase.testing


class TestInheritanceController(layabase.CRUDController):
    class Inherited:
        optional = sqlalchemy.Column(sqlalchemy.String)

    class TestInheritanceModel(Inherited):
        __tablename__ = "inheritance_table_name"

        key = sqlalchemy.Column(
            sqlalchemy.Integer, primary_key=True, autoincrement=True
        )
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)

    model = TestInheritanceModel
    audit = True


@pytest.fixture
def db():
    _db = layabase.load("sqlite:///:memory:", [TestInheritanceController])
    yield _db
    layabase.testing.reset(_db)


def test_inheritance_does_not_throw_errors(db):
    pass
