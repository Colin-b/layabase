import datetime

import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        unique_key = layabase.mongo.Column(str, is_primary_key=True)
        non_unique_key = layabase.mongo.Column(
            datetime.date, index_type=layabase.mongo.IndexType.Other
        )
        _id = layabase.mongo.Column()

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_post_many_with_same_unique_index_is_invalid(controller):
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post_many(
            [
                {"unique_key": "test", "non_unique_key": "2017-01-01", "_id": "1" * 24},
                {"unique_key": "test", "non_unique_key": "2017-01-01", "_id": "2" * 24},
            ]
        )
    assert exception_info.value.errors == {
        "": [
            "{'writeErrors': [{'index': 1, 'code': 11000, 'errmsg': 'E11000 "
            "Duplicate Key Error', 'op': {'unique_key': 'test', 'non_unique_key': "
            "datetime.datetime(2017, 1, 1, 0, 0, tzinfo=datetime.timezone.utc), '_id': "
            "ObjectId('222222222222222222222222')}}], 'nInserted': 1}"
        ]
    }
    assert exception_info.value.received_data == [
        {
            "_id": "111111111111111111111111",
            "non_unique_key": "2017-01-01",
            "unique_key": "test",
        },
        {
            "_id": "222222222222222222222222",
            "non_unique_key": "2017-01-01",
            "unique_key": "test",
        },
    ]
