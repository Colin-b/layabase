import pytest
import sqlalchemy

import layabase
import layabase.testing


@pytest.fixture
def controller():
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    return layabase.CRUDController(TestTable)


@pytest.fixture
def database(controller):
    return layabase.load("sqlite:///:memory:", [controller])


def test_reset_cleanup_content(controller: layabase.CRUDController, database):
    controller.post_many([{"key": "1"}, {"key": "2"}])
    assert len(controller.get({})) == 2
    layabase.testing.reset(database)
    assert len(controller.get({})) == 0
