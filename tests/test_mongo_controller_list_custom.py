import pytest

import layabase
import layabase.mongo


@pytest.fixture
def controller():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(is_primary_key=True)
        list_field = layabase.mongo.Column(list)

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])
    return controller


def test_post_list_of_dict_is_valid(controller):
    assert controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    ) == {
        "key": "my_key",
        "list_field": [
            {"first_key": "key1", "second_key": 1},
            {"first_key": "key2", "second_key": 2},
        ],
    }


def test_post_optional_missing_list_of_dict_is_valid(controller):
    assert controller.post({"key": "my_key"}) == {"key": "my_key", "list_field": None}


def test_post_optional_list_of_dict_as_none_is_valid(controller):
    assert controller.post({"key": "my_key", "list_field": None}) == {
        "key": "my_key",
        "list_field": None,
    }


def test_get_list_of_dict_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert controller.get(
        {
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ]
        }
    ) == [
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    ]


def test_get_optional_list_of_dict_as_none_is_skipped(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert controller.get({"list_field": None}) == [
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    ]


def test_delete_list_of_dict_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert (
        controller.delete(
            {
                "list_field": [
                    {"first_key": "key1", "second_key": 1},
                    {"first_key": "key2", "second_key": 2},
                ]
            }
        )
        == 1
    )


def test_delete_optional_list_of_dict_as_none_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert controller.delete({"list_field": None}) == 1


def test_put_list_of_dict_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert controller.put(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key2", "second_key": 10},
                {"first_key": "key1", "second_key": 2},
            ],
        }
    ) == (
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        },
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key2", "second_key": 10},
                {"first_key": "key1", "second_key": 2},
            ],
        },
    )


def test_put_without_optional_list_of_dict_is_valid(controller):
    controller.post(
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        }
    )
    assert controller.put({"key": "my_key"}) == (
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        },
        {
            "key": "my_key",
            "list_field": [
                {"first_key": "key1", "second_key": 1},
                {"first_key": "key2", "second_key": 2},
            ],
        },
    )
