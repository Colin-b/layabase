import pytest
import sqlalchemy
from sqlalchemy.orm.query import Query

import layabase


@pytest.fixture
def controller():
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
        optional = sqlalchemy.Column(sqlalchemy.String)

        @classmethod
        def customize_query(cls, query: Query) -> Query:
            return query.filter(cls.optional == "test")

    controller = layabase.CRUDController(TestTable)
    layabase.load("sqlite:///:memory:", [controller])
    return controller


def test_custom_query(controller: layabase.CRUDController):
    controller.post_many(
        [
            {"key": "my_key1", "mandatory": 1, "optional": "test"},
            {"key": "my_key2", "mandatory": 2, "optional": "test2"},
        ]
    )

    assert controller.get({}) == [
        {"key": "my_key1", "mandatory": 1, "optional": "test"}
    ]
