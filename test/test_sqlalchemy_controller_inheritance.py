import pytest
import sqlalchemy

import layabase


@pytest.fixture
def controller():
    class Inherited:
        optional = sqlalchemy.Column(sqlalchemy.String)

    class TestInheritanceModel(Inherited):
        __tablename__ = "test"

        key = sqlalchemy.Column(
            sqlalchemy.Integer, primary_key=True, autoincrement=True
        )
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)

    controller = layabase.CRUDController(TestInheritanceModel, audit=True)
    layabase.load("sqlite:///:memory:", [controller])
    return controller


def test_inheritance_does_not_throw_errors(controller):
    pass
