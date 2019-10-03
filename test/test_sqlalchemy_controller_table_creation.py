import sqlalchemy

import layabase


def test_tables_are_added_to_metadata():
    class TestTable:
        __tablename__ = "test"

        key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

        @classmethod
        def _post_init(cls, base):
            pass

    db = layabase.load("sqlite:///:memory:", [layabase.CRUDController(TestTable)])
    assert "sqlite:///:memory:" == str(db.metadata.bind.engine.url)
    assert ["test"] == list(db.metadata.tables.keys())
