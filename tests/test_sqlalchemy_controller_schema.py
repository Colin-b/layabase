import pytest
import sqlalchemy

import layabase


@pytest.fixture
def controller() -> layabase.CRUDController:
    class TestTable:
        __tablename__ = "test"
        __table_args__ = {u"schema": "schema_name"}

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

    controller = layabase.CRUDController(TestTable)
    layabase.load("sqlite:///:memory:", [controller])
    return controller


def test_get_model_description_returns_description(controller: layabase.CRUDController):
    assert controller.get_model_description() == {
        "key": "key",
        "mandatory": "mandatory",
        "optional": "optional",
        "schema": "schema_name",
        "table": "test",
    }
