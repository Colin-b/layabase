import pytest
import sqlalchemy

import layabase
import layabase.testing


@pytest.fixture
def controller():
    class TestController(layabase.CRUDController):
        class TestLikeOperatorModel:
            __tablename__ = "like_operator_table_name"

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

        model = TestLikeOperatorModel
        interpret_star_character = True

    _db = layabase.load("sqlite:///:memory:", [TestController])
    yield TestController
    layabase.testing.reset(_db)


def test_get_like_operator_double_star(controller):
    controller.post_many([{"key": "my_key"}, {"key": "my_key2"}, {"key": "my_ey"}])
    assert [{"key": "my_key"}, {"key": "my_key2"}] == controller.get({"key": "*y_k*"})


def test_get_like_operator_star_at_start(controller):
    controller.post_many(
        [{"key": "my_key"}, {"key": "my_key2"}, {"key": "my_ey"}, {"key": "my_k"}]
    )
    assert [{"key": "my_k"}] == controller.get({"key": "*y_k"})


def test_get_like_operator_star_at_end(controller):
    controller.post_many(
        [
            {"key": "my_key"},
            {"key": "my_key2"},
            {"key": "my_ey"},
            {"key": "my_k"},
            {"key": "y_key"},
        ]
    )
    assert [{"key": "y_key"}] == controller.get({"key": "y_k*"})


def test_get_like_operator_no_star(controller):
    controller.post_many(
        [
            {"key": "my_key"},
            {"key": "my_key2"},
            {"key": "my_ey"},
            {"key": "my_k"},
            {"key": "y_key"},
        ]
    )
    assert [{"key": "my_key"}] == controller.get({"key": "my_key"})


def test_get_like_operator_no_star_no_result(controller):
    controller.post_many(
        [
            {"key": "my_key"},
            {"key": "my_key2"},
            {"key": "my_ey"},
            {"key": "my_k"},
            {"key": "y_key"},
        ]
    )
    assert [] == controller.get({"key": "y_k"})
