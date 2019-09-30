import pytest
import sqlalchemy

import layabase
import layabase.testing


class TestLikeOperatorController(layabase.CRUDController):
    class TestLikeOperatorModel:
        __tablename__ = "like_operator_table_name"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

    model = TestLikeOperatorModel
    interpret_star_character = True


@pytest.fixture
def db():
    _db = layabase.load("sqlite:///:memory:", [TestLikeOperatorController])
    yield _db
    layabase.testing.reset(_db)


def test_get_like_operator_double_star(db):
    TestLikeOperatorController.post_many(
        [{"key": "my_key"}, {"key": "my_key2"}, {"key": "my_ey"}]
    )
    assert [{"key": "my_key"}, {"key": "my_key2"}] == TestLikeOperatorController.get(
        {"key": "*y_k*"}
    )


def test_get_like_operator_star_at_start(db):
    TestLikeOperatorController.post_many(
        [{"key": "my_key"}, {"key": "my_key2"}, {"key": "my_ey"}, {"key": "my_k"}]
    )
    assert [{"key": "my_k"}] == TestLikeOperatorController.get({"key": "*y_k"})


def test_get_like_operator_star_at_end(db):
    TestLikeOperatorController.post_many(
        [
            {"key": "my_key"},
            {"key": "my_key2"},
            {"key": "my_ey"},
            {"key": "my_k"},
            {"key": "y_key"},
        ]
    )
    assert [{"key": "y_key"}] == TestLikeOperatorController.get({"key": "y_k*"})


def test_get_like_operator_no_star(db):
    TestLikeOperatorController.post_many(
        [
            {"key": "my_key"},
            {"key": "my_key2"},
            {"key": "my_ey"},
            {"key": "my_k"},
            {"key": "y_key"},
        ]
    )
    assert [{"key": "my_key"}] == TestLikeOperatorController.get({"key": "my_key"})


def test_get_like_operator_no_star_no_result(db):
    TestLikeOperatorController.post_many(
        [
            {"key": "my_key"},
            {"key": "my_key2"},
            {"key": "my_ey"},
            {"key": "my_k"},
            {"key": "y_key"},
        ]
    )
    assert [] == TestLikeOperatorController.get({"key": "y_k"})
