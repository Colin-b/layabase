import pytest

import layabase
import layabase.mongo


def test_post_failure_in_get_fields():
    should_fail = False

    def get_fields_failure(document):
        if should_fail:
            raise Exception("Original failure reason")
        return {}

    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        dict_col = layabase.mongo.DictColumn(get_fields=get_fields_failure)

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])

    should_fail = True
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"key": "my_key", "dict_col": {}})
    assert exception_info.value.errors == {"dict_col": ["Original failure reason"]}
    assert exception_info.value.received_data == {"dict_col": {}, "key": "my_key"}


def test_put_failure_in_get_fields():
    should_fail = False

    def get_fields_failure(document):
        if should_fail:
            raise Exception("Original failure reason")
        return {}

    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        dict_col = layabase.mongo.DictColumn(get_fields=get_fields_failure)

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])

    controller.post({"key": "my_key", "dict_col": {}})
    should_fail = True
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.put({"key": "my_key", "dict_col": {}})
    assert exception_info.value.errors == {"dict_col": ["Original failure reason"]}
    assert exception_info.value.received_data == {"dict_col": {}, "key": "my_key"}


def test_get_failure_in_get_fields():
    should_fail = False

    def get_fields_failure(document):
        if should_fail:
            raise Exception("Original failure reason")
        return {}

    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str, is_primary_key=True)
        dict_col = layabase.mongo.DictColumn(get_fields=get_fields_failure)

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])

    controller.post({"key": "my_key", "dict_col": {}})
    should_fail = True
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.get({"key": "my_key", "dict_col": {}})
    assert exception_info.value.errors == {"dict_col": ["Original failure reason"]}
    assert exception_info.value.received_data == {"dict_col": {}, "key": "my_key"}


def test_with_index_fields():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str)
        dict_col = layabase.mongo.DictColumn(
            fields={
                "field1": layabase.mongo.Column(),
                "field2": layabase.mongo.Column(),
            },
            index_fields={
                "field2": layabase.mongo.Column(
                    index_type=layabase.mongo.IndexType.Unique
                )
            },
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])

    controller.post({"key": "my_key1", "dict_col": {"field2": "test"}})
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"key": "my_key2", "dict_col": {"field2": "test"}})
    assert exception_info.value.errors == {"": ["This document already exists."]}
    assert exception_info.value.received_data == {
        "key": "my_key2",
        "dict_col": {"field2": "test", "field1": None},
    }


def test_with_get_index_fields():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str)
        dict_col = layabase.mongo.DictColumn(
            fields={
                "field1": layabase.mongo.Column(),
                "field2": layabase.mongo.Column(),
            },
            get_index_fields=lambda document: {
                "field2": layabase.mongo.Column(
                    index_type=layabase.mongo.IndexType.Unique
                )
            },
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])

    controller.post({"key": "my_key1", "dict_col": {"field2": "test"}})
    with pytest.raises(layabase.ValidationFailed) as exception_info:
        controller.post({"key": "my_key2", "dict_col": {"field2": "test"}})
    assert exception_info.value.errors == {"": ["This document already exists."]}
    assert exception_info.value.received_data == {
        "key": "my_key2",
        "dict_col": {"field2": "test", "field1": None},
    }


def test_without_index_fields():
    class TestCollection:
        __collection_name__ = "test"

        key = layabase.mongo.Column(str)
        dict_col = layabase.mongo.DictColumn(
            fields={
                "field1": layabase.mongo.Column(),
                "field2": layabase.mongo.Column(),
            }
        )

    controller = layabase.CRUDController(TestCollection)
    layabase.load("mongomock", [controller])

    controller.post({"key": "my_key1", "dict_col": {"field2": "test"}})
    assert controller.post({"key": "my_key2", "dict_col": {"field2": "test"}}) == {
        "key": "my_key2",
        "dict_col": {"field2": "test", "field1": None},
    }
