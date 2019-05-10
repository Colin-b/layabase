import datetime
import enum
import json
import logging
import sys
import tempfile
import unittest
from threading import Thread
from typing import List, Dict

import pymongo
from flask_restplus import inputs
from pycommon_error.validation import ValidationFailed, ModelCouldNotBeFound
from pycommon_test import mock_now
from pycommon_test.flask_restplus_mock import TestAPI

logging.basicConfig(
    format="%(asctime)s [%(threadName)s] [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.DEBUG,
)

from pycommon_database import database, database_mongo, versioning_mongo

logger = logging.getLogger(__name__)


def parser_types(flask_parser) -> dict:
    return {arg.name: arg.type for arg in flask_parser.args}


def parser_actions(flask_parser) -> dict:
    return {arg.name: arg.action for arg in flask_parser.args}


class MongoColumnTest(unittest.TestCase):
    def setUp(self):
        logger.info(f"-------------------------------")
        logger.info(f"Start of {self._testMethodName}")

    def tearDown(self):
        logger.info(f"End of {self._testMethodName}")
        logger.info(f"-------------------------------")

    def test_str_column_cannot_auto_increment(self):
        with self.assertRaises(Exception) as cm:
            database_mongo.Column(should_auto_increment=True)
        self.assertEqual("Only int fields can be auto incremented.", str(cm.exception))

    def test_auto_incremented_field_cannot_be_non_nullable(self):
        with self.assertRaises(Exception) as cm:
            database_mongo.Column(int, should_auto_increment=True, is_nullable=False)
        self.assertEqual(
            "A field cannot be mandatory and auto incremented at the same time.",
            str(cm.exception),
        )

    def test_field_with_default_value_cannot_be_non_nullable(self):
        with self.assertRaises(Exception) as cm:
            database_mongo.Column(default_value="test", is_nullable=False)
        self.assertEqual(
            "A field cannot be mandatory and having a default value at the same time.",
            str(cm.exception),
        )

    def test_spaces_are_forbidden_at_start_of_column_name(self):
        with self.assertRaises(Exception) as cm:
            database_mongo.Column(name="   test")
        self.assertEqual(
            "   test is not a valid name. Spaces are not allowed at start or end of field names.",
            str(cm.exception),
        )

    def test_spaces_are_forbidden_at_end_of_column_name(self):
        with self.assertRaises(Exception) as cm:
            database_mongo.Column(name="test   ")
        self.assertEqual(
            "test    is not a valid name. Spaces are not allowed at start or end of field names.",
            str(cm.exception),
        )

    def test_spaces_are_forbidden_at_start_and_end_of_column_name(self):
        with self.assertRaises(Exception) as cm:
            database_mongo.Column(name="   test   ")
        self.assertEqual(
            "   test    is not a valid name. Spaces are not allowed at start or end of field names.",
            str(cm.exception),
        )


class MongoDatabaseTest(unittest.TestCase):
    def setUp(self):
        logger.info(f"-------------------------------")
        logger.info(f"Start of {self._testMethodName}")

    def tearDown(self):
        logger.info(f"End of {self._testMethodName}")
        logger.info(f"-------------------------------")

    def test_none_connection_string_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            database.load(None, None)
        self.assertEqual(
            "A database connection URL must be provided.", str(cm.exception)
        )

    def test_empty_connection_string_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            database.load("", None)
        self.assertEqual(
            "A database connection URL must be provided.", str(cm.exception)
        )

    def test_no_create_models_function_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            database.load("mongomock", None)
        self.assertEqual(
            "A method allowing to create related models must be provided.",
            str(cm.exception),
        )


class EnumTest(enum.Enum):
    Value1 = 1
    Value2 = 2


class MongoCRUDControllerTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    class TestStrictController(database.CRUDController):
        pass

    class TestAutoIncrementController(database.CRUDController):
        pass

    class TestDateController(database.CRUDController):
        pass

    class TestDictController(database.CRUDController):
        pass

    class TestOptionalDictController(database.CRUDController):
        pass

    class TestIndexController(database.CRUDController):
        pass

    class TestDefaultPrimaryKeyController(database.CRUDController):
        pass

    class TestListController(database.CRUDController):
        pass

    class TestStringListController(database.CRUDController):
        pass

    class TestLimitsController(database.CRUDController):
        pass

    class TestIdController(database.CRUDController):
        pass

    class TestUnvalidatedListAndDictController(database.CRUDController):
        pass

    class TestVersionedController(database.CRUDController):
        pass

    class TestNullableAutoSetController(database.CRUDController):
        pass

    class TestVersionedUniqueNonPrimaryController(database.CRUDController):
        pass

    class TestUniqueNonPrimaryController(database.CRUDController):
        pass

    class TestIntAndFloatController(database.CRUDController):
        pass

    class TestDictInDictController(database.CRUDController):
        pass

    class TestNoneInsertController(database.CRUDController):
        pass

    class TestNoneRetrieveController(database.CRUDController):
        pass

    class TestNoneNotInsertedController(database.CRUDController):
        pass

    class TestDictRequiredNonNullableVersionedController(database.CRUDController):
        pass

    class TestSupportForComparisonSignsController(database.CRUDController):
        pass

    _db = None

    @classmethod
    def setUpClass(cls):
        mock_now()
        cls._db = database.load("mongomock", cls._create_models)
        cls.TestController.namespace(TestAPI)
        cls.TestStrictController.namespace(TestAPI)
        cls.TestAutoIncrementController.namespace(TestAPI)
        cls.TestDateController.namespace(TestAPI)
        cls.TestDictController.namespace(TestAPI)
        cls.TestOptionalDictController.namespace(TestAPI)
        cls.TestIndexController.namespace(TestAPI)
        cls.TestDefaultPrimaryKeyController.namespace(TestAPI)
        cls.TestListController.namespace(TestAPI)
        cls.TestStringListController.namespace(TestAPI)
        cls.TestLimitsController.namespace(TestAPI)
        cls.TestIdController.namespace(TestAPI)
        cls.TestUnvalidatedListAndDictController.namespace(TestAPI)
        cls.TestVersionedController.namespace(TestAPI)
        cls.TestDictRequiredNonNullableVersionedController.namespace(TestAPI)
        cls.TestNullableAutoSetController.namespace(TestAPI)
        cls.TestVersionedUniqueNonPrimaryController.namespace(TestAPI)
        cls.TestUniqueNonPrimaryController.namespace(TestAPI)
        cls.TestIntAndFloatController.namespace(TestAPI)
        cls.TestDictInDictController.namespace(TestAPI)
        cls.TestNoneInsertController.namespace(TestAPI)
        cls.TestNoneRetrieveController.namespace(TestAPI)
        cls.TestNoneNotInsertedController.namespace(TestAPI)
        cls.TestSupportForComparisonSignsController.namespace(TestAPI)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info("Declare model class...")
        class TestModel(
            database_mongo.CRUDModel, base=base, table_name="sample_table_name"
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            mandatory = database_mongo.Column(int, is_nullable=False)
            optional = database_mongo.Column(str)

        class TestStrictModel(
            database_mongo.CRUDModel,
            base=base,
            table_name="strict_table_name",
            skip_unknown_fields=False,
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            mandatory = database_mongo.Column(int, is_nullable=False)
            optional = database_mongo.Column(str)

        class TestAutoIncrementModel(
            database_mongo.CRUDModel, base=base, table_name="auto_increment_table_name"
        ):
            key = database_mongo.Column(
                int, is_primary_key=True, should_auto_increment=True
            )
            enum_field = database_mongo.Column(
                EnumTest, is_nullable=False, description="Test Documentation"
            )
            optional_with_default = database_mongo.Column(
                str, default_value="Test value"
            )

        class TestDateModel(
            database_mongo.CRUDModel, base=base, table_name="date_table_name"
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            date_str = database_mongo.Column(datetime.date)
            datetime_str = database_mongo.Column(datetime.datetime)

        class TestDictModel(
            database_mongo.CRUDModel, base=base, table_name="dict_table_name"
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            dict_col = database_mongo.DictColumn(
                fields={
                    "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                    "second_key": database_mongo.Column(int, is_nullable=False),
                },
                is_nullable=False,
            )

        class TestOptionalDictModel(
            database_mongo.CRUDModel, base=base, table_name="optional_dict_table_name"
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            dict_col = database_mongo.DictColumn(
                get_fields=lambda model_as_dict: {
                    "first_key": database_mongo.Column(EnumTest, is_nullable=True),
                    "second_key": database_mongo.Column(int, is_nullable=True),
                }
            )

        class TestIndexModel(
            database_mongo.CRUDModel, base=base, table_name="index_table_name"
        ):
            unique_key = database_mongo.Column(str, is_primary_key=True)
            non_unique_key = database_mongo.Column(
                datetime.date, index_type=database_mongo.IndexType.Other
            )

        class TestDefaultPrimaryKeyModel(
            database_mongo.CRUDModel, base=base, table_name="default_primary_table_name"
        ):
            key = database_mongo.Column(is_primary_key=True, default_value="test")
            optional = database_mongo.Column()

        class TestListModel(
            database_mongo.CRUDModel, base=base, table_name="list_table_name"
        ):
            key = database_mongo.Column(is_primary_key=True)
            list_field = database_mongo.ListColumn(
                database_mongo.DictColumn(
                    fields={
                        "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                        "second_key": database_mongo.Column(int, is_nullable=False),
                    }
                )
            )
            bool_field = database_mongo.Column(bool)

        class TestStringListModel(
            database_mongo.CRUDModel, base=base, table_name="string_list_table_name"
        ):
            key = database_mongo.Column(is_primary_key=True)
            list_field = database_mongo.ListColumn(database_mongo.Column(), sorted=True)

        class TestLimitsModel(
            database_mongo.CRUDModel, base=base, table_name="limits_table_name"
        ):
            key = database_mongo.Column(is_primary_key=True, min_length=3, max_length=4)
            list_field = database_mongo.Column(
                list, min_length=2, max_length=3, example=["my", "test"]
            )
            int_field = database_mongo.Column(int, min_value=100, max_value=999)

        class TestUnvalidatedListAndDictModel(
            database_mongo.CRUDModel, base=base, table_name="list_and_dict_table_name"
        ):
            float_key = database_mongo.Column(float, is_primary_key=True)
            float_with_default = database_mongo.Column(float, default_value=34)
            dict_field = database_mongo.Column(dict, is_required=True)
            list_field = database_mongo.Column(list, is_required=True)

        class TestIdModel(
            database_mongo.CRUDModel, base=base, table_name="id_table_name"
        ):
            _id = database_mongo.Column(is_primary_key=True)

        class TestNullableAutoSetModel(
            database_mongo.CRUDModel,
            base=base,
            table_name="nullable_auto_set_table_name",
        ):
            prim_def_inc = database_mongo.Column(
                int, is_primary_key=True, default_value=1, should_auto_increment=True
            )
            prim_def = database_mongo.Column(int, is_primary_key=True, default_value=1)
            prim_inc = database_mongo.Column(
                int, is_primary_key=True, should_auto_increment=True
            )

        class TestVersionedModel(
            versioning_mongo.VersionedCRUDModel,
            base=base,
            table_name="versioned_table_name",
        ):
            key = database_mongo.Column(is_primary_key=True)
            dict_field = database_mongo.DictColumn(
                fields={
                    "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                    "second_key": database_mongo.Column(int, is_nullable=False),
                },
                is_required=True,
            )

        class TestDictRequiredNonNullableVersionedModel(
            versioning_mongo.VersionedCRUDModel,
            base=base,
            table_name="req_not_null_versioned_table_name",
        ):
            key = database_mongo.Column(is_primary_key=True)
            dict_field = database_mongo.DictColumn(
                fields={
                    "first_key": database_mongo.Column(EnumTest, is_nullable=False),
                    "second_key": database_mongo.Column(int, is_nullable=False),
                },
                is_required=True,
                is_nullable=False,
            )

        class TestVersionedUniqueNonPrimaryModel(
            versioning_mongo.VersionedCRUDModel,
            base=base,
            table_name="versioned_uni_table_name",
        ):
            key = database_mongo.Column(int, should_auto_increment=True)
            unique = database_mongo.Column(
                int, index_type=database_mongo.IndexType.Unique
            )

        class TestUniqueNonPrimaryModel(
            database_mongo.CRUDModel, base=base, table_name="uni_table_name"
        ):
            key = database_mongo.Column(int, should_auto_increment=True)
            unique = database_mongo.Column(
                int, index_type=database_mongo.IndexType.Unique
            )

        class TestIntAndFloatModel(
            database_mongo.CRUDModel, base=base, table_name="int_and_float"
        ):
            int_value = database_mongo.Column(int)
            float_value = database_mongo.Column(float)

        class TestDictInDictModel(
            database_mongo.CRUDModel, base=base, table_name="dict_in_dict_table_name"
        ):
            key = database_mongo.Column(is_primary_key=True)
            dict_field = database_mongo.DictColumn(
                fields={
                    "first_key": database_mongo.DictColumn(
                        fields={
                            "inner_key1": database_mongo.Column(
                                EnumTest, is_nullable=False
                            ),
                            "inner_key2": database_mongo.Column(int, is_nullable=False),
                        },
                        is_required=True,
                    ),
                    "second_key": database_mongo.Column(int, is_nullable=False),
                },
                is_required=True,
            )

        class TestNoneNotInsertedModel(
            database_mongo.CRUDModel, base=base, table_name="none_table_name"
        ):
            key = database_mongo.Column(int, is_primary_key=True)
            my_dict = database_mongo.DictColumn(
                fields={"null_value": database_mongo.Column(store_none=False)},
                is_required=True,
            )

        class TestNoneInsertModel(
            database_mongo.CRUDModel,
            base=base,
            table_name="none_table_name",
            skip_name_check=True,
        ):
            key = database_mongo.Column(int, is_primary_key=True)
            my_dict = database_mongo.DictColumn(
                fields={"null_value": database_mongo.Column(store_none=True)},
                is_required=True,
            )

        class TestNoneRetrieveModel(
            database_mongo.CRUDModel,
            base=base,
            table_name="none_table_name",
            skip_name_check=True,
        ):
            key = database_mongo.Column(int, is_primary_key=True)
            my_dict = database_mongo.Column(dict, is_required=True)

        class TestSupportForComparisonSignsModel(
            database_mongo.CRUDModel, base=base, table_name="int_and_float"
        ):
            int_value = database_mongo.Column(int)
            float_value = database_mongo.Column(float)
            date_value = database_mongo.Column(datetime.date)
            datetime_value = database_mongo.Column(datetime.datetime)

        logger.info("Save model class...")
        cls.TestController.model(TestModel)
        cls.TestStrictController.model(TestStrictModel)
        cls.TestAutoIncrementController.model(TestAutoIncrementModel)
        cls.TestDateController.model(TestDateModel)
        cls.TestDictController.model(TestDictModel)
        cls.TestOptionalDictController.model(TestOptionalDictModel)
        cls.TestIndexController.model(TestIndexModel)
        cls.TestDefaultPrimaryKeyController.model(TestDefaultPrimaryKeyModel)
        cls.TestListController.model(TestListModel)
        cls.TestStringListController.model(TestStringListModel)
        cls.TestLimitsController.model(TestLimitsModel)
        cls.TestIdController.model(TestIdModel)
        cls.TestUnvalidatedListAndDictController.model(TestUnvalidatedListAndDictModel)
        cls.TestNullableAutoSetController.model(TestNullableAutoSetModel)
        cls.TestVersionedController.model(TestVersionedModel)
        cls.TestDictRequiredNonNullableVersionedController.model(
            TestDictRequiredNonNullableVersionedModel
        )
        cls.TestVersionedUniqueNonPrimaryController.model(
            TestVersionedUniqueNonPrimaryModel
        )
        cls.TestUniqueNonPrimaryController.model(TestUniqueNonPrimaryModel)
        cls.TestIntAndFloatController.model(TestIntAndFloatModel)
        cls.TestDictInDictController.model(TestDictInDictModel)
        cls.TestNoneNotInsertedController.model(TestNoneNotInsertedModel)
        cls.TestNoneInsertController.model(TestNoneInsertModel)
        cls.TestNoneRetrieveController.model(TestNoneRetrieveModel)
        cls.TestSupportForComparisonSignsController.model(TestSupportForComparisonSignsModel)
        return [
            TestModel,
            TestStrictModel,
            TestAutoIncrementModel,
            TestDateModel,
            TestDictModel,
            TestOptionalDictModel,
            TestIndexModel,
            TestDefaultPrimaryKeyModel,
            TestListModel,
            TestStringListModel,
            TestLimitsModel,
            TestIdModel,
            TestUnvalidatedListAndDictModel,
            TestVersionedModel,
            TestDictRequiredNonNullableVersionedModel,
            TestNullableAutoSetModel,
            TestVersionedUniqueNonPrimaryModel,
            TestUniqueNonPrimaryModel,
            TestIntAndFloatModel,
            TestDictInDictModel,
            TestNoneInsertModel,
            TestSupportForComparisonSignsModel,
        ]

    def setUp(self):
        logger.info(f"-------------------------------")
        logger.info(f"Start of {self._testMethodName}")
        database.reset(self._db)
        self.initial_command = self._db.command

    def tearDown(self):
        self._db.command = self.initial_command
        logger.info(f"End of {self._testMethodName}")
        logger.info(f"-------------------------------")

    def test_health_details_failure(self):
        def fail_ping(*args):
            raise Exception("Unable to ping")

        self._db.command = fail_ping
        self.assertEqual(
            (
                "fail",
                {
                    "mongomock:ping": {
                        "componentType": "datastore",
                        "output": "Unable to ping",
                        "status": "fail",
                        "time": "2018-10-11T15:05:05.663979",
                    }
                },
            ),
            database.health_details(self._db),
        )

    def test_health_details_success(self):
        self.assertEqual(
            (
                "pass",
                {
                    "mongomock:ping": {
                        "componentType": "datastore",
                        "observedValue": {"ok": 1.0},
                        "status": "pass",
                        "time": "2018-10-11T15:05:05.663979",
                    }
                },
            ),
            database.health_details(self._db),
        )

    def test_get_all_without_data_returns_empty_list(self):
        self.assertEqual([], self.TestController.get({}))

    def test_post_with_nothing_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.post(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual(None, cm.exception.received_data)

    def test_post_list_with_nothing_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.post_many(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual([], cm.exception.received_data)

    def test_post_with_empty_dict_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.post({})
        self.assertEqual(
            {
                "key": ["Missing data for required field."],
                "mandatory": ["Missing data for required field."],
            },
            cm.exception.errors,
        )
        self.assertEqual({}, cm.exception.received_data)

    def test_post_with_list_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.post([""])
        self.assertEqual({"": ["Must be a dictionary."]}, cm.exception.errors)
        self.assertEqual([""], cm.exception.received_data)

    def test_get_url_with_primary_key_in_model_and_many_models(self):
        models = [
            {
                "key": "first",
                "dict_field": {"first_key": "Value1", "second_key": 1},
                "valid_since_revision": 1,
                "valid_until_revision": -1,
            },
            {
                "key": "second",
                "dict_field": {"first_key": "Value2", "second_key": 2},
                "valid_since_revision": 1,
                "valid_until_revision": -1,
            },
        ]
        self.assertEqual(
            "/test?key=first&key=second",
            self.TestVersionedController.get_url("/test", *models),
        )

    def test_get_url_with_primary_key_in_model_and_a_single_model(self):
        model = {
            "key": "first",
            "dict_field": {"first_key": "Value1", "second_key": 1},
            "valid_since_revision": 1,
            "valid_until_revision": -1,
        }
        self.assertEqual(
            "/test?key=first", self.TestVersionedController.get_url("/test", model)
        )

    def test_get_url_with_primary_key_in_model_and_no_model(self):
        self.assertEqual("/test", self.TestVersionedController.get_url("/test"))

    def test_get_url_without_primary_key_in_model_and_many_models(self):
        models = [{"key": 1, "unique": 2}, {"key": 2, "unique": 3}]
        self.assertEqual(
            "/test", self.TestUniqueNonPrimaryController.get_url("/test", *models)
        )

    def test_get_url_without_primary_key_in_model_and_one_model(self):
        model = {"key": 1, "unique": 2}
        self.assertEqual(
            "/test", self.TestUniqueNonPrimaryController.get_url("/test", model)
        )

    def test_get_url_without_primary_key_in_model_and_no_model(self):
        self.assertEqual("/test", self.TestUniqueNonPrimaryController.get_url("/test"))

    def test_post_many_with_dict_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.post_many({""})
        self.assertEqual({"": ["Must be a list of dictionaries."]}, cm.exception.errors)
        self.assertEqual({""}, cm.exception.received_data)

    def test_put_with_list_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.put([""])
        self.assertEqual({"": ["Must be a dictionary."]}, cm.exception.errors)
        self.assertEqual([""], cm.exception.received_data)

    def test_put_many_with_dict_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.put_many({""})
        self.assertEqual({"": ["Must be a list."]}, cm.exception.errors)
        self.assertEqual({""}, cm.exception.received_data)

    def test_post_many_with_empty_list_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.post_many([])
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual([], cm.exception.received_data)

    def test_put_with_nothing_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.put(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual(None, cm.exception.received_data)

    def test_put_with_empty_dict_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.put({})
        self.assertEqual(
            {"key": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual({}, cm.exception.received_data)

    def test_delete_without_nothing_do_not_fail(self):
        self.assertEqual(0, self.TestController.delete({}))

    def test_post_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({"key": "my_key"})
        self.assertEqual(
            {"mandatory": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual({"key": "my_key"}, cm.exception.received_data)

    def test_post_many_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{"key": "my_key"}])
        self.assertEqual(
            {0: {"mandatory": ["Missing data for required field."]}},
            cm.exception.errors,
        )
        self.assertEqual([{"key": "my_key"}], cm.exception.received_data)

    def test_post_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({"mandatory": 1})
        self.assertEqual(
            {"key": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual({"mandatory": 1}, cm.exception.received_data)

    def test_post_many_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{"mandatory": 1}])
        self.assertEqual(
            {0: {"key": ["Missing data for required field."]}}, cm.exception.errors
        )
        self.assertEqual([{"mandatory": 1}], cm.exception.received_data)

    def test_post_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post(
                {"key": datetime.date(2007, 12, 5), "mandatory": 1}
            )
        self.assertEqual({"key": ["Not a valid str."]}, cm.exception.errors)
        self.assertEqual(
            {"key": datetime.date(2007, 12, 5), "mandatory": 1},
            cm.exception.received_data,
        )

    def test_post_int_instead_of_str_is_valid(self):
        self.assertEqual(
            {"key": "3", "mandatory": 1, "optional": None},
            self.TestController.post({"key": 3, "mandatory": 1}),
        )

    def test_post_boolean_instead_of_str_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({"key": True, "mandatory": 1})
        self.assertEqual({"key": ["Not a valid str."]}, cm.exception.errors)
        self.assertEqual({"key": True, "mandatory": 1}, cm.exception.received_data)

    def test_post_float_instead_of_str_is_valid(self):
        self.assertEqual(
            {"key": "1.5", "mandatory": 1, "optional": None},
            self.TestController.post({"key": 1.5, "mandatory": 1}),
        )

    def test_post_twice_with_unique_index_is_invalid(self):
        self.assertEqual(
            {"non_unique_key": "2017-01-01", "unique_key": "test"},
            self.TestIndexController.post(
                {"unique_key": "test", "non_unique_key": "2017-01-01"}
            ),
        )
        with self.assertRaises(Exception) as cm:
            self.TestIndexController.post(
                {"unique_key": "test", "non_unique_key": "2017-01-02"}
            )
        self.assertEqual({"": ["This document already exists."]}, cm.exception.errors)
        self.assertEqual(
            {"non_unique_key": "2017-01-02", "unique_key": "test"},
            cm.exception.received_data,
        )

    def test_get_all_without_primary_key_is_valid(self):
        self.assertEqual(
            {"non_unique_key": "2017-01-01", "unique_key": "test"},
            self.TestIndexController.post(
                {"unique_key": "test", "non_unique_key": "2017-01-01"}
            ),
        )
        self.assertEqual(
            [{"non_unique_key": "2017-01-01", "unique_key": "test"}],
            self.TestIndexController.get({}),
        )

    def test_get_one_and_multiple_results_is_invalid(self):
        self.TestIndexController.post(
            {"unique_key": "test", "non_unique_key": "2017-01-01"}
        )
        self.TestIndexController.post(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"}
        )
        with self.assertRaises(Exception) as cm:
            self.TestIndexController.get_one({})
        self.assertEqual(
            {"": ["More than one result: Consider another filtering."]},
            cm.exception.errors,
        )
        self.assertEqual({}, cm.exception.received_data)

    def test_get_one_is_valid(self):
        self.TestIndexController.post(
            {"unique_key": "test", "non_unique_key": "2017-01-01"}
        )
        self.TestIndexController.post(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"}
        )
        self.assertEqual(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"},
            self.TestIndexController.get_one({"unique_key": "test2"}),
        )

    def test_get_with_list_is_valid(self):
        self.TestIndexController.post(
            {"unique_key": "test", "non_unique_key": "2017-01-01"}
        )
        self.TestIndexController.post(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"}
        )
        self.assertEqual(
            [
                {"unique_key": "test", "non_unique_key": "2017-01-01"},
                {"unique_key": "test2", "non_unique_key": "2017-01-01"},
            ],
            self.TestIndexController.get({"unique_key": ["test", "test2"]}),
        )

    def test_get_with_partial_matching_list_is_valid(self):
        self.TestIndexController.post(
            {"unique_key": "test", "non_unique_key": "2017-01-01"}
        )
        self.TestIndexController.post(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"}
        )
        self.assertEqual(
            [{"unique_key": "test2", "non_unique_key": "2017-01-01"}],
            self.TestIndexController.get({"unique_key": ["test2"]}),
        )

    def test_get_with_empty_list_is_valid(self):
        self.TestIndexController.post(
            {"unique_key": "test", "non_unique_key": "2017-01-01"}
        )
        self.TestIndexController.post(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"}
        )
        self.assertEqual(
            [
                {"unique_key": "test", "non_unique_key": "2017-01-01"},
                {"unique_key": "test2", "non_unique_key": "2017-01-01"},
            ],
            self.TestIndexController.get({"unique_key": []}),
        )

    def test_get_with_partialy_matching_and_not_matching_list_is_valid(self):
        self.TestIndexController.post(
            {"unique_key": "test", "non_unique_key": "2017-01-01"}
        )
        self.TestIndexController.post(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"}
        )
        self.assertEqual(
            [{"unique_key": "test", "non_unique_key": "2017-01-01"}],
            self.TestIndexController.get(
                {"unique_key": ["not existing", "test", "another non existing"]}
            ),
        )

    def test_post_int_str_in_int_column(self):
        self.assertEqual(
            {"int_value": 15, "float_value": 1.0},
            self.TestIntAndFloatController.post(
                {"int_value": "15", "float_value": 1.0}
            ),
        )

    def test_put_int_str_in_int_column(self):
        self.TestIntAndFloatController.post({"int_value": 15, "float_value": 1.0})
        self.assertEqual(
            (
                {"int_value": 15, "float_value": 1.0},
                {"int_value": 16, "float_value": 1.0},
            ),
            self.TestIntAndFloatController.put({"int_value": "16", "float_value": 1.0}),
        )

    def test_put_without_primary_and_incremented_field(self):
        self.TestNullableAutoSetController.post({"prim_def": 1})
        with self.assertRaises(Exception) as cm:
            self.TestNullableAutoSetController.put({"prim_def": 1})
        self.assertEqual(
            {"prim_inc": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual({"prim_def": 1}, cm.exception.received_data)

    def test_delete_int_str_in_int_column(self):
        self.TestIntAndFloatController.post({"int_value": 15, "float_value": 1.0})
        self.assertEqual(1, self.TestIntAndFloatController.delete({"int_value": "15"}))

    def test_post_float_str_in_float_column(self):
        self.assertEqual(
            {"int_value": 15, "float_value": 1.3},
            self.TestIntAndFloatController.post(
                {"int_value": 15, "float_value": "1.3"}
            ),
        )

    def test_put_float_str_in_float_column(self):
        self.TestIntAndFloatController.post({"int_value": 15, "float_value": 1.3})
        self.assertEqual(
            (
                {"int_value": 15, "float_value": 1.3},
                {"int_value": 15, "float_value": 1.4},
            ),
            self.TestIntAndFloatController.put({"int_value": 15, "float_value": "1.4"}),
        )

    def test_delete_float_str_in_float_column(self):
        self.TestIntAndFloatController.post({"int_value": 15, "float_value": 1.3})
        self.assertEqual(
            1, self.TestIntAndFloatController.delete({"float_value": "1.3"})
        )

    def test_post_with_non_int_str_in_int_column(self):
        with self.assertRaises(Exception) as cm:
            self.TestIntAndFloatController.post(
                {"int_value": "abc", "float_value": 1.0}
            )
        self.assertEqual({"int_value": ["Not a valid int."]}, cm.exception.errors)
        self.assertEqual(
            {"int_value": "abc", "float_value": 1.0}, cm.exception.received_data
        )

    def test_post_with_non_float_str_in_float_column(self):
        with self.assertRaises(Exception) as cm:
            self.TestIntAndFloatController.post({"int_value": 1, "float_value": "abc"})
        self.assertEqual({"float_value": ["Not a valid float."]}, cm.exception.errors)
        self.assertEqual(
            {"float_value": "abc", "int_value": 1}, cm.exception.received_data
        )

    def test_get_with_non_int_str_in_int_column(self):
        with self.assertRaises(Exception) as cm:
            self.TestIntAndFloatController.get({"int_value": "abc", "float_value": 1.0})
        self.assertEqual({"int_value": ["Not a valid int."]}, cm.exception.errors)
        self.assertEqual(
            {"int_value": "abc", "float_value": 1.0}, cm.exception.received_data
        )

    def test_get_with_non_float_str_in_float_column(self):
        with self.assertRaises(Exception) as cm:
            self.TestIntAndFloatController.get({"int_value": 1, "float_value": "abc"})
        self.assertEqual({"float_value": ["Not a valid float."]}, cm.exception.errors)
        self.assertEqual(
            {"float_value": "abc", "int_value": 1}, cm.exception.received_data
        )

    def test_put_with_non_int_str_in_int_column(self):
        with self.assertRaises(Exception) as cm:
            self.TestIntAndFloatController.put({"int_value": "abc", "float_value": 1.0})
        self.assertEqual({"int_value": ["Not a valid int."]}, cm.exception.errors)
        self.assertEqual(
            {"int_value": "abc", "float_value": 1.0}, cm.exception.received_data
        )

    def test_put_with_non_float_str_in_float_column(self):
        with self.assertRaises(Exception) as cm:
            self.TestIntAndFloatController.put({"int_value": 1, "float_value": "abc"})
        self.assertEqual({"float_value": ["Not a valid float."]}, cm.exception.errors)
        self.assertEqual(
            {"float_value": "abc", "int_value": 1}, cm.exception.received_data
        )

    def test_delete_with_non_int_str_in_int_column(self):
        with self.assertRaises(Exception) as cm:
            self.TestIntAndFloatController.delete(
                {"int_value": "abc", "float_value": 1.0}
            )
        self.assertEqual({"int_value": ["Not a valid int."]}, cm.exception.errors)
        self.assertEqual(
            {"int_value": "abc", "float_value": 1.0}, cm.exception.received_data
        )

    def test_delete_with_non_float_str_in_float_column(self):
        with self.assertRaises(Exception) as cm:
            self.TestIntAndFloatController.delete(
                {"int_value": 1, "float_value": "abc"}
            )
        self.assertEqual({"float_value": ["Not a valid float."]}, cm.exception.errors)
        self.assertEqual(
            {"float_value": "abc", "int_value": 1}, cm.exception.received_data
        )

    def test_get_is_valid_with_int_str_in_int_column(self):
        self.TestIntAndFloatController.post({"int_value": 123, "float_value": 1.0})
        self.assertEqual(
            {"int_value": 123, "float_value": 1.0},
            self.TestIntAndFloatController.get_one({"int_value": "123"}),
        )

    def test_get_retrieve_none_field_when_not_in_model(self):
        self.TestNoneInsertController.post({"key": 1, "my_dict": {"null_value": None}})
        self.assertEqual(
            [{"key": 1, "my_dict": {"null_value": None}}],
            self.TestNoneRetrieveController.get({}),
        )

    def test_get_do_not_retrieve_none_field_when_not_in_model(self):
        self.TestNoneNotInsertedController.post(
            {"key": 1, "my_dict": {"null_value": None}}
        )
        self.assertEqual(
            [{"key": 1, "my_dict": {}}], self.TestNoneRetrieveController.get({})
        )

    def test_get_is_valid_with_float_str_in_float_column(self):
        self.TestIntAndFloatController.post({"int_value": 1, "float_value": 1.23})
        self.assertEqual(
            {"int_value": 1, "float_value": 1.23},
            self.TestIntAndFloatController.get_one({"float_value": "1.23"}),
        )

    def test_delete_with_list_is_valid(self):
        self.TestIndexController.post(
            {"unique_key": "test", "non_unique_key": "2017-01-01"}
        )
        self.TestIndexController.post(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"}
        )
        self.assertEqual(
            2, self.TestIndexController.delete({"unique_key": ["test", "test2"]})
        )

    def test_delete_with_partial_matching_list_is_valid(self):
        self.TestIndexController.post(
            {"unique_key": "test", "non_unique_key": "2017-01-01"}
        )
        self.TestIndexController.post(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"}
        )
        self.assertEqual(1, self.TestIndexController.delete({"unique_key": ["test2"]}))

    def test_non_iso8601_date_failure(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestIndexController.post(
                {"unique_key": "test", "non_unique_key": "12/06/2017"}
            )
        self.assertEqual({"non_unique_key": ["Not a valid date."]}, cm.exception.errors)
        self.assertEqual(
            {"unique_key": "test", "non_unique_key": "12/06/2017"},
            cm.exception.received_data,
        )

    def test_delete_with_empty_list_is_valid(self):
        self.TestIndexController.post(
            {"unique_key": "test", "non_unique_key": "2017-01-01"}
        )
        self.TestIndexController.post(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"}
        )
        self.assertEqual(2, self.TestIndexController.delete({"unique_key": []}))

    def test_delete_with_partialy_matching_and_not_matching_list_is_valid(self):
        self.TestIndexController.post(
            {"unique_key": "test", "non_unique_key": "2017-01-01"}
        )
        self.TestIndexController.post(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"}
        )
        self.assertEqual(
            1,
            self.TestIndexController.delete(
                {"unique_key": ["not existing", "test", "another non existing"]}
            ),
        )

    def test_get_one_without_result_is_valid(self):
        self.TestIndexController.post(
            {"unique_key": "test", "non_unique_key": "2017-01-01"}
        )
        self.TestIndexController.post(
            {"unique_key": "test2", "non_unique_key": "2017-01-01"}
        )
        self.assertEqual({}, self.TestIndexController.get_one({"unique_key": "test3"}))

    def test_get_field_names_valid(self):
        self.assertEqual(
            ["non_unique_key", "unique_key"], self.TestIndexController.get_field_names()
        )

    def _assert_regex(self, expected, actual):
        self.assertRegex(
            f"{actual}",
            f"{expected}".replace("[", "\\[").replace("]", "\\]").replace("\\\\", "\\"),
        )

    def test_post_versioning_is_valid(self):
        self.assertEqual(
            {
                "key": "first",
                "dict_field": {"first_key": "Value1", "second_key": 1},
                "valid_since_revision": 1,
                "valid_until_revision": -1,
            },
            self.TestVersionedController.post(
                {
                    "key": "first",
                    "dict_field.first_key": EnumTest.Value1,
                    "dict_field.second_key": 1,
                }
            ),
        )
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 1,
                    "valid_until_revision": -1,
                }
            ],
            self.TestVersionedController.get_history({}),
        )
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 1,
                    "valid_until_revision": -1,
                }
            ],
            self.TestVersionedController.get({}),
        )

    def test_post_without_providing_required_nullable_dict_column_is_valid(self):
        self.assertEqual(
            {
                "dict_field": {"first_key": None, "second_key": None},
                "key": "first",
                "valid_since_revision": 1,
                "valid_until_revision": -1,
            },
            self.TestVersionedController.post({"key": "first"}),
        )

    def test_post_without_providing_required_non_nullable_dict_column_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestDictRequiredNonNullableVersionedController.post({"key": "first"})
        self.assertEqual(
            {"dict_field": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual({"key": "first"}, cm.exception.received_data)

    def test_put_without_providing_required_nullable_dict_column_is_valid(self):
        self.TestVersionedController.post(
            {"key": "first", "dict_field": {"first_key": "Value1", "second_key": 0}}
        )
        self.assertEqual(
            (
                {
                    "dict_field": {"first_key": "Value1", "second_key": 0},
                    "key": "first",
                    "valid_since_revision": 1,
                    "valid_until_revision": -1,
                },
                {
                    "dict_field": {"first_key": "Value1", "second_key": 0},
                    "key": "first",
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                },
            ),
            self.TestVersionedController.put({"key": "first"}),
        )

    def test_put_without_providing_required_non_nullable_dict_column_is_valid(self):
        self.TestDictRequiredNonNullableVersionedController.post(
            {"key": "first", "dict_field": {"first_key": "Value1", "second_key": 0}}
        )
        self.assertEqual(
            (
                {
                    "dict_field": {"first_key": "Value1", "second_key": 0},
                    "key": "first",
                    "valid_since_revision": 1,
                    "valid_until_revision": -1,
                },
                {
                    "dict_field": {"first_key": "Value1", "second_key": 0},
                    "key": "first",
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                },
            ),
            self.TestDictRequiredNonNullableVersionedController.put({"key": "first"}),
        )

    def test_put_with_null_provided_required_non_nullable_dict_column_is_invalid(self):
        self.TestDictRequiredNonNullableVersionedController.post(
            {"key": "first", "dict_field": {"first_key": "Value1", "second_key": 0}}
        )
        with self.assertRaises(ValidationFailed) as cm:
            self.TestDictRequiredNonNullableVersionedController.put(
                {"key": "first", "dict_field": None}
            )
        self.assertEqual(
            {"dict_field": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual(
            {"key": "first", "dict_field": None}, cm.exception.received_data
        )

    def test_revison_is_shared(self):
        self.TestVersionedController.post(
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.TestVersionedUniqueNonPrimaryController.post({"unique": 1})
        self.TestVersionedController.put({"key": "first", "dict_field.second_key": 2})
        self.TestVersionedController.delete({"key": "first"})
        self.TestVersionedController.rollback_to({"revision": 2})
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 2},
                    "valid_since_revision": 3,
                    "valid_until_revision": 4,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 1,
                    "valid_until_revision": 3,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 5,
                    "valid_until_revision": -1,
                },
            ],
            self.TestVersionedController.get_history({}),
        )
        self.assertEqual(
            [
                {
                    "key": 1,
                    "unique": 1,
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                }
            ],
            self.TestVersionedUniqueNonPrimaryController.get_history({}),
        )

    def test_put_versioning_is_valid(self):
        self.TestVersionedController.post(
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.assertEqual(
            (
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 1,
                    "valid_until_revision": -1,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                },
            ),
            self.TestVersionedController.put(
                {"key": "first", "dict_field.first_key": EnumTest.Value2}
            ),
        )
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 1,
                    "valid_until_revision": 2,
                },
            ],
            self.TestVersionedController.get_history({}),
        )
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                }
            ],
            self.TestVersionedController.get({}),
        )

    def test_delete_versioning_is_valid(self):
        self.TestVersionedController.post(
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.TestVersionedController.put(
            {"key": "first", "dict_field.first_key": EnumTest.Value2}
        )
        self.assertEqual(1, self.TestVersionedController.delete({"key": "first"}))
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 2,
                    "valid_until_revision": 3,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 1,
                    "valid_until_revision": 2,
                },
            ],
            self.TestVersionedController.get_history({}),
        )
        self.assertEqual([], self.TestVersionedController.get({}))

    def test_rollback_deleted_versioning_is_valid(self):
        self.TestVersionedController.post(
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.TestVersionedController.put(
            {"key": "first", "dict_field.first_key": EnumTest.Value2}
        )
        before_delete = 2
        self.TestVersionedController.delete({"key": "first"})
        self.assertEqual(
            1, self.TestVersionedController.rollback_to({"revision": before_delete})
        )
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 2,
                    "valid_until_revision": 3,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 1,
                    "valid_until_revision": 2,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 4,
                    "valid_until_revision": -1,
                },
            ],
            self.TestVersionedController.get_history({}),
        )
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 4,
                    "valid_until_revision": -1,
                }
            ],
            self.TestVersionedController.get({}),
        )

    def test_rollback_before_update_deleted_versioning_is_valid(self):
        self.TestVersionedController.post(
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        before_update = 1
        self.TestVersionedController.put(
            {"key": "first", "dict_field.first_key": EnumTest.Value2}
        )
        self.TestVersionedController.delete({"key": "first"})
        self.assertEqual(
            1, self.TestVersionedController.rollback_to({"revision": before_update})
        )
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 2,
                    "valid_until_revision": 3,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 1,
                    "valid_until_revision": 2,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 4,
                    "valid_until_revision": -1,
                },
            ],
            self.TestVersionedController.get_history({}),
        )
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 4,
                    "valid_until_revision": -1,
                }
            ],
            self.TestVersionedController.get({}),
        )

    def test_rollback_already_valid_versioning_is_valid(self):
        self.TestVersionedController.post(
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.TestVersionedController.put(
            {"key": "first", "dict_field.first_key": EnumTest.Value2}
        )

        self.assertEqual(0, self.TestVersionedController.rollback_to({"revision": 2}))
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 1,
                    "valid_until_revision": 2,
                },
            ],
            self.TestVersionedController.get_history({}),
        )
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                }
            ],
            self.TestVersionedController.get({}),
        )

    def test_rollback_unknown_criteria_is_valid(self):
        self.TestVersionedController.post(
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        before_update = 1
        self.TestVersionedController.put(
            {"key": "first", "dict_field.first_key": EnumTest.Value2}
        )

        self.assertEqual(
            0,
            self.TestVersionedController.rollback_to(
                {"revision": before_update, "key": "unknown"}
            ),
        )
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 1,
                    "valid_until_revision": 2,
                },
            ],
            self.TestVersionedController.get_history({}),
        )
        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                }
            ],
            self.TestVersionedController.get({}),
        )

    def test_versioned_many(self):
        self.TestVersionedController.post_many(
            [
                {
                    "key": "first",
                    "dict_field.first_key": EnumTest.Value1,
                    "dict_field.second_key": 1,
                },
                {
                    "key": "second",
                    "dict_field.first_key": EnumTest.Value2,
                    "dict_field.second_key": 2,
                },
            ]
        )
        self.TestVersionedController.put_many(
            [
                {"key": "first", "dict_field.first_key": EnumTest.Value2},
                {"key": "second", "dict_field.second_key": 3},
            ]
        )

        self.assertEqual(
            [
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                },
                {
                    "key": "second",
                    "dict_field": {"first_key": "Value2", "second_key": 3},
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                },
                {
                    "key": "first",
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "valid_since_revision": 1,
                    "valid_until_revision": 2,
                },
                {
                    "key": "second",
                    "dict_field": {"first_key": "Value2", "second_key": 2},
                    "valid_since_revision": 1,
                    "valid_until_revision": 2,
                },
            ],
            self.TestVersionedController.get_history({}),
        )

    def test_rollback_without_revision_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestVersionedController.rollback_to({"key": "unknown"})
        self.assertEqual(
            {"revision": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual({"key": "unknown"}, cm.exception.received_data)

    def test_rollback_with_non_int_revision_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestVersionedController.rollback_to({"revision": "invalid revision"})
        self.assertEqual({"revision": ["Not a valid int."]}, cm.exception.errors)
        self.assertEqual({"revision": "invalid revision"}, cm.exception.received_data)

    def test_rollback_without_versioning_is_valid(self):
        self.assertEqual(
            0, self.TestController.rollback_to({"revision": "invalid revision"})
        )

    def test_rollback_with_negative_revision_is_valid(self):
        self.assertEqual(0, self.TestVersionedController.rollback_to({"revision": -1}))

    def test_rollback_before_existing_is_valid(self):
        self.TestVersionedController.post(
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        before_insert = 1
        self.TestVersionedController.post(
            {
                "key": "second",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.assertEqual(
            1, self.TestVersionedController.rollback_to({"revision": before_insert})
        )
        self.assertEqual([], self.TestVersionedController.get({"key": "second"}))

    def test_get_revision_is_valid_when_empty(self):
        self.assertEqual(0, self.TestVersionedController._model.current_revision())

    def test_get_revision_is_valid_when_1(self):
        self.TestVersionedController.post(
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.assertEqual(1, self.TestVersionedController._model.current_revision())

    def test_get_revision_is_valid_when_2(self):
        self.TestVersionedController.post(
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.TestVersionedController.post(
            {
                "key": "second",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.assertEqual(2, self.TestVersionedController._model.current_revision())

    def test_rollback_to_0(self):
        self.TestVersionedController.post(
            {
                "key": "first",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.TestVersionedController.post(
            {
                "key": "second",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.assertEqual(2, self.TestVersionedController.rollback_to({"revision": 0}))
        self.assertEqual([], self.TestVersionedController.get({}))

    def test_rollback_multiple_rows_is_valid(self):
        self.TestVersionedController.post(
            {
                "key": "1",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.TestVersionedController.post(
            {
                "key": "2",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.TestVersionedController.put(
            {"key": "1", "dict_field.first_key": EnumTest.Value2}
        )
        self.TestVersionedController.delete({"key": "2"})
        self.TestVersionedController.post(
            {
                "key": "3",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.TestVersionedController.post(
            {
                "key": "4",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        before_insert = 6
        self.TestVersionedController.post(
            {
                "key": "5",
                "dict_field.first_key": EnumTest.Value1,
                "dict_field.second_key": 1,
            }
        )
        self.TestVersionedController.put({"key": "1", "dict_field.second_key": 2})
        self.assertEqual(
            2,  # Remove key 5 and Update key 1 (Key 3 and Key 4 unchanged)
            self.TestVersionedController.rollback_to({"revision": before_insert}),
        )
        self.assertEqual(
            [
                {
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "key": "3",
                    "valid_since_revision": 5,
                    "valid_until_revision": -1,
                },
                {
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "key": "4",
                    "valid_since_revision": 6,
                    "valid_until_revision": -1,
                },
                {
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "key": "1",
                    "valid_since_revision": 9,
                    "valid_until_revision": -1,
                },
            ],
            self.TestVersionedController.get({}),
        )
        self.maxDiff = None
        self.assertEqual(
            [
                {
                    "dict_field": {"first_key": "Value2", "second_key": 2},
                    "key": "1",
                    "valid_since_revision": 8,
                    "valid_until_revision": 9,
                },
                {
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "key": "2",
                    "valid_since_revision": 2,
                    "valid_until_revision": 4,
                },
                {
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "key": "1",
                    "valid_since_revision": 1,
                    "valid_until_revision": 3,
                },
                {
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "key": "3",
                    "valid_since_revision": 5,
                    "valid_until_revision": -1,
                },
                {
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "key": "4",
                    "valid_since_revision": 6,
                    "valid_until_revision": -1,
                },
                {
                    "dict_field": {"first_key": "Value1", "second_key": 1},
                    "key": "5",
                    "valid_since_revision": 7,
                    "valid_until_revision": 9,
                },
                {
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "key": "1",
                    "valid_since_revision": 3,
                    "valid_until_revision": 8,
                },
                {
                    "dict_field": {"first_key": "Value2", "second_key": 1},
                    "key": "1",
                    "valid_since_revision": 9,
                    "valid_until_revision": -1,
                },
            ],
            self.TestVersionedController.get_history({}),
        )

    def test_versioning_handles_unique_non_primary(self):
        self.TestVersionedUniqueNonPrimaryController.post({"unique": 1})
        with self.assertRaises(Exception) as cm:
            self.TestVersionedUniqueNonPrimaryController.post({"unique": 1})
        self.assertEqual({"": ["This document already exists."]}, cm.exception.errors)
        self.assertEqual(
            {
                "key": 2,
                "unique": 1,
                "valid_since_revision": 2,
                "valid_until_revision": -1,
            },
            cm.exception.received_data,
        )

    def test_insert_to_non_unique_after_update(self):
        self.TestVersionedUniqueNonPrimaryController.post({"unique": 1})
        self.TestVersionedUniqueNonPrimaryController.put({"key": 1, "unique": 2})
        with self.assertRaises(Exception) as cm:
            self.TestVersionedUniqueNonPrimaryController.post({"unique": 2})
        self.assertEqual({"": ["This document already exists."]}, cm.exception.errors)
        self.assertEqual(
            {
                "key": 2,
                "unique": 2,
                "valid_since_revision": 3,
                "valid_until_revision": -1,
            },
            cm.exception.received_data,
        )

    def test_update_to_non_unique_versioned(self):
        self.TestVersionedUniqueNonPrimaryController.post({"unique": 1})
        self.TestVersionedUniqueNonPrimaryController.post({"unique": 2})
        with self.assertRaises(Exception) as cm:
            self.TestVersionedUniqueNonPrimaryController.put({"key": 1, "unique": 2})
        self.assertEqual({"": ["This document already exists."]}, cm.exception.errors)
        self.assertEqual(
            {
                "key": 1,
                "unique": 2,
                "valid_since_revision": 3,
                "valid_until_revision": -1,
            },
            cm.exception.received_data,
        )

    def test_update_to_non_unique(self):
        self.TestUniqueNonPrimaryController.post({"unique": 1})
        self.TestUniqueNonPrimaryController.post({"unique": 2})
        with self.assertRaises(Exception) as cm:
            self.TestUniqueNonPrimaryController.put({"unique": 2, "key": 1})
        self.assertEqual({"": ["This document already exists."]}, cm.exception.errors)
        self.assertEqual({"key": 1, "unique": 2}, cm.exception.received_data)

    def test_post_id_is_valid(self):
        self.assertEqual(
            {"_id": "123456789abcdef012345678"},
            self.TestIdController.post({"_id": "123456789ABCDEF012345678"}),
        )

    def test_invalid_id_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestIdController.post({"_id": "invalid value"})
        self.assertEqual(
            {
                "_id": [
                    "'invalid value' is not a valid ObjectId, it must be a 12-byte input or a 24-character hex string"
                ]
            },
            cm.exception.errors,
        )
        self.assertEqual({"_id": "invalid value"}, cm.exception.received_data)

    def test_get_all_with_none_primary_key_is_valid(self):
        self.assertEqual(
            {"non_unique_key": "2017-01-01", "unique_key": "test"},
            self.TestIndexController.post(
                {"unique_key": "test", "non_unique_key": "2017-01-01"}
            ),
        )
        self.assertEqual(
            [{"non_unique_key": "2017-01-01", "unique_key": "test"}],
            self.TestIndexController.get({"unique_key": None}),
        )

    def test_post_many_with_same_unique_index_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestIndexController.post_many(
                [
                    {"unique_key": "test", "non_unique_key": "2017-01-01"},
                    {"unique_key": "test", "non_unique_key": "2017-01-01"},
                ]
            )
        self.assertRegex(
            str(cm.exception.errors[""][0]),
            "{'writeErrors': [{'index': 1, 'code': 11000, 'errmsg': 'E11000 Duplicate Key Error', 'op': {'unique_key': 'test', 'non_unique_key': "
            "datetime.datetime(2017, 1, 1, 0, 0), '_id': ObjectId('.*')}}], 'nInserted': 1}".replace(
                "[", "\["
            )
            .replace("]", "\]")
            .replace("(", "\(")
            .replace(")", "\)"),
        )
        self.assertEqual(
            [
                {"unique_key": "test", "non_unique_key": "2017-01-01"},
                {"unique_key": "test", "non_unique_key": "2017-01-01"},
            ],
            cm.exception.received_data,
        )

    def test_post_without_primary_key_but_default_value_is_valid(self):
        self.assertEqual(
            {"key": "test", "optional": "test2"},
            self.TestDefaultPrimaryKeyController.post({"optional": "test2"}),
        )

    def test_get_on_default_value_is_valid(self):
        self.TestDefaultPrimaryKeyController.post({"optional": "test"})
        self.TestDefaultPrimaryKeyController.post({"key": "test2", "optional": "test2"})
        self.assertEqual(
            [{"key": "test", "optional": "test"}],
            self.TestDefaultPrimaryKeyController.get({"key": "test"}),
        )

    def test_delete_on_default_value_is_valid(self):
        self.TestDefaultPrimaryKeyController.post({"optional": "test"})
        self.TestDefaultPrimaryKeyController.post({"key": "test2", "optional": "test2"})
        self.assertEqual(
            1, self.TestDefaultPrimaryKeyController.delete({"key": "test"})
        )

    def test_put_without_primary_key_but_default_value_is_valid(self):
        self.assertEqual(
            {"key": "test", "optional": "test2"},
            self.TestDefaultPrimaryKeyController.post({"optional": "test2"}),
        )
        self.assertEqual(
            (
                {"key": "test", "optional": "test2"},
                {"key": "test", "optional": "test3"},
            ),
            self.TestDefaultPrimaryKeyController.put({"optional": "test3"}),
        )

    def test_post_different_unique_index_is_valid(self):
        self.assertEqual(
            {"non_unique_key": "2017-01-01", "unique_key": "test"},
            self.TestIndexController.post(
                {"unique_key": "test", "non_unique_key": "2017-01-01"}
            ),
        )
        self.assertEqual(
            {"non_unique_key": "2017-01-01", "unique_key": "test2"},
            self.TestIndexController.post(
                {"unique_key": "test2", "non_unique_key": "2017-01-01"}
            ),
        )
        self.assertEqual(
            [
                {"non_unique_key": "2017-01-01", "unique_key": "test"},
                {"non_unique_key": "2017-01-01", "unique_key": "test2"},
            ],
            self.TestIndexController.get({}),
        )

    def test_post_many_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many(
                [{"key": datetime.date(2007, 12, 5), "mandatory": 1}]
            )
        self.assertEqual({0: {"key": ["Not a valid str."]}}, cm.exception.errors)
        self.assertEqual(
            [{"key": datetime.date(2007, 12, 5), "mandatory": 1}],
            cm.exception.received_data,
        )

    def test_json_post_model_versioned(self):
        self.assertEqual(
            "TestVersionedModel_Versioned",
            self.TestVersionedController.json_post_model.name,
        )
        self.assertEqual(
            {
                "dict_field": (
                    "Nested",
                    {"first_key": "String", "second_key": "Integer"},
                ),
                "key": "String",
            },
            self.TestVersionedController.json_post_model.fields_flask_type,
        )
        self.assertEqual(
            {
                "dict_field": (None, {"first_key": None, "second_key": None}),
                "key": None,
            },
            self.TestVersionedController.json_post_model.fields_description,
        )
        self.assertEqual(
            {
                "dict_field": (
                    None,
                    {"first_key": ["Value1", "Value2"], "second_key": None},
                ),
                "key": None,
            },
            self.TestVersionedController.json_post_model.fields_enum,
        )
        self.assertEqual(
            {
                "dict_field": (
                    {"first_key": "Value1", "second_key": 1},
                    {"first_key": "Value1", "second_key": 1},
                ),
                "key": "sample key",
            },
            self.TestVersionedController.json_post_model.fields_example,
        )
        self.assertEqual(
            {
                "dict_field": (
                    {"first_key": None, "second_key": None},
                    {"first_key": None, "second_key": None},
                ),
                "key": None,
            },
            self.TestVersionedController.json_post_model.fields_default,
        )
        self.assertEqual(
            {
                "dict_field": (True, {"first_key": False, "second_key": False}),
                "key": False,
            },
            self.TestVersionedController.json_post_model.fields_required,
        )
        self.assertEqual(
            {
                "dict_field": (False, {"first_key": False, "second_key": False}),
                "key": False,
            },
            self.TestVersionedController.json_post_model.fields_readonly,
        )

    def test_json_post_model_with_list_of_dict(self):
        self.assertEqual("TestListModel", self.TestListController.json_post_model.name)
        self.assertEqual(
            {
                "bool_field": "Boolean",
                "key": "String",
                "list_field": (
                    "List",
                    {
                        "list_field_inner": (
                            "Nested",
                            {"first_key": "String", "second_key": "Integer"},
                        )
                    },
                ),
            },
            self.TestListController.json_post_model.fields_flask_type,
        )
        self.assertEqual(
            {
                "bool_field": None,
                "key": None,
                "list_field": (
                    None,
                    {
                        "list_field_inner": (
                            None,
                            {"first_key": None, "second_key": None},
                        )
                    },
                ),
            },
            self.TestListController.json_post_model.fields_description,
        )
        self.assertEqual(
            {
                "bool_field": None,
                "key": None,
                "list_field": (
                    None,
                    {
                        "list_field_inner": (
                            None,
                            {"first_key": ["Value1", "Value2"], "second_key": None},
                        )
                    },
                ),
            },
            self.TestListController.json_post_model.fields_enum,
        )
        self.assertEqual(
            {
                "bool_field": True,
                "key": "sample key",
                "list_field": (
                    [{"first_key": "Value1", "second_key": 1}],
                    {
                        "list_field_inner": (
                            {"first_key": "Value1", "second_key": 1},
                            {"first_key": "Value1", "second_key": 1},
                        )
                    },
                ),
            },
            self.TestListController.json_post_model.fields_example,
        )
        self.assertEqual(
            {
                "bool_field": None,
                "key": None,
                "list_field": (
                    None,
                    {
                        "list_field_inner": (
                            {"first_key": None, "second_key": None},
                            {"first_key": None, "second_key": None},
                        )
                    },
                ),
            },
            self.TestListController.json_post_model.fields_default,
        )
        self.assertEqual(
            {
                "bool_field": False,
                "key": False,
                "list_field": (
                    False,
                    {
                        "list_field_inner": (
                            False,
                            {"first_key": False, "second_key": False},
                        )
                    },
                ),
            },
            self.TestListController.json_post_model.fields_required,
        )
        self.assertEqual(
            {
                "bool_field": False,
                "key": False,
                "list_field": (
                    False,
                    {
                        "list_field_inner": (
                            False,
                            {"first_key": False, "second_key": False},
                        )
                    },
                ),
            },
            self.TestListController.json_post_model.fields_readonly,
        )

    def test_json_put_model_with_list_of_dict(self):
        self.assertEqual("TestListModel", self.TestListController.json_put_model.name)
        self.assertEqual(
            {
                "bool_field": "Boolean",
                "key": "String",
                "list_field": (
                    "List",
                    {
                        "list_field_inner": (
                            "Nested",
                            {"first_key": "String", "second_key": "Integer"},
                        )
                    },
                ),
            },
            self.TestListController.json_put_model.fields_flask_type,
        )
        self.assertEqual(
            {
                "bool_field": None,
                "key": None,
                "list_field": (
                    None,
                    {
                        "list_field_inner": (
                            None,
                            {"first_key": None, "second_key": None},
                        )
                    },
                ),
            },
            self.TestListController.json_put_model.fields_description,
        )
        self.assertEqual(
            {
                "bool_field": None,
                "key": None,
                "list_field": (
                    None,
                    {
                        "list_field_inner": (
                            None,
                            {"first_key": ["Value1", "Value2"], "second_key": None},
                        )
                    },
                ),
            },
            self.TestListController.json_put_model.fields_enum,
        )
        self.assertEqual(
            {
                "bool_field": True,
                "key": "sample key",
                "list_field": (
                    [{"first_key": "Value1", "second_key": 1}],
                    {
                        "list_field_inner": (
                            {"first_key": "Value1", "second_key": 1},
                            {"first_key": "Value1", "second_key": 1},
                        )
                    },
                ),
            },
            self.TestListController.json_put_model.fields_example,
        )
        self.assertEqual(
            {
                "bool_field": None,
                "key": None,
                "list_field": (
                    None,
                    {
                        "list_field_inner": (
                            {"first_key": None, "second_key": None},
                            {"first_key": None, "second_key": None},
                        )
                    },
                ),
            },
            self.TestListController.json_put_model.fields_default,
        )
        self.assertEqual(
            {
                "bool_field": False,
                "key": False,
                "list_field": (
                    False,
                    {
                        "list_field_inner": (
                            False,
                            {"first_key": False, "second_key": False},
                        )
                    },
                ),
            },
            self.TestListController.json_put_model.fields_required,
        )
        self.assertEqual(
            {
                "bool_field": False,
                "key": False,
                "list_field": (
                    False,
                    {
                        "list_field_inner": (
                            False,
                            {"first_key": False, "second_key": False},
                        )
                    },
                ),
            },
            self.TestListController.json_put_model.fields_readonly,
        )

    def test_put_with_wrong_type_is_invalid(self):
        self.TestController.post({"key": "value1", "mandatory": 1})
        with self.assertRaises(Exception) as cm:
            self.TestController.put({"key": "value1", "mandatory": "invalid value"})
        self.assertEqual({"mandatory": ["Not a valid int."]}, cm.exception.errors)
        self.assertEqual(
            {"key": "value1", "mandatory": "invalid value"}, cm.exception.received_data
        )

    def test_put_with_optional_as_None_is_valid(self):
        self.TestController.post({"key": "value1", "mandatory": 1})
        self.TestController.put({"key": "value1", "mandatory": 1, "optional": None})
        self.assertEqual(
            [{"mandatory": 1, "key": "value1", "optional": None}],
            self.TestController.get({}),
        )

    def test_put_with_non_nullable_as_None_is_invalid(self):
        self.TestController.post({"key": "value1", "mandatory": 1})
        with self.assertRaises(Exception) as cm:
            self.TestController.put({"key": "value1", "mandatory": None})
        self.assertEqual(
            {"mandatory": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual(
            {"key": "value1", "mandatory": None}, cm.exception.received_data
        )

    def test_post_without_optional_is_valid(self):
        self.assertEqual(
            {"mandatory": 1, "key": "my_key", "optional": None},
            self.TestController.post({"key": "my_key", "mandatory": 1}),
        )

    def test_get_with_non_nullable_None_is_valid(self):
        self.assertEqual(
            {"mandatory": 1, "key": "my_key", "optional": None},
            self.TestController.post({"key": "my_key", "mandatory": 1}),
        )
        self.assertEqual(
            [{"mandatory": 1, "key": "my_key", "optional": None}],
            self.TestController.get({"key": "my_key", "mandatory": None}),
        )

    def test_post_many_without_optional_is_valid(self):
        self.assertEqual(
            [
                {"mandatory": 1, "key": "my_key", "optional": None},
                {"mandatory": 2, "key": "my_key2", "optional": None},
            ],
            self.TestController.post_many(
                [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
            ),
        )

    def test_put_many_is_valid(self):
        self.TestController.post_many(
            [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
        )
        self.assertEqual(
            (
                [
                    {"mandatory": 1, "key": "my_key", "optional": None},
                    {"mandatory": 2, "key": "my_key2", "optional": None},
                ],
                [
                    {"mandatory": 1, "key": "my_key", "optional": "test"},
                    {"mandatory": 3, "key": "my_key2", "optional": None},
                ],
            ),
            self.TestController.put_many(
                [
                    {"key": "my_key", "optional": "test"},
                    {"key": "my_key2", "mandatory": 3},
                ]
            ),
        )

    def test_post_with_optional_is_valid(self):
        self.assertEqual(
            {"mandatory": 1, "key": "my_key", "optional": "my_value"},
            self.TestController.post(
                {"key": "my_key", "mandatory": 1, "optional": "my_value"}
            ),
        )

    def test_post_list_of_dict_is_valid(self):
        self.assertEqual(
            {
                "bool_field": False,
                "key": "my_key",
                "list_field": [
                    {"first_key": "Value1", "second_key": 1},
                    {"first_key": "Value2", "second_key": 2},
                ],
            },
            self.TestListController.post(
                {
                    "key": "my_key",
                    "list_field": [
                        {"first_key": EnumTest.Value1, "second_key": 1},
                        {"first_key": EnumTest.Value2, "second_key": 2},
                    ],
                    "bool_field": False,
                }
            ),
        )

    def test_post_list_of_str_is_sorted(self):
        self.assertEqual(
            {"key": "my_key", "list_field": ["a", "b", "c"]},
            self.TestStringListController.post(
                {"key": "my_key", "list_field": ["c", "a", "b"]}
            ),
        )

    def test_within_limits_is_valid(self):
        self.assertEqual(
            {"int_field": 100, "key": "111", "list_field": ["1", "2", "3"]},
            self.TestLimitsController.post(
                {"key": "111", "list_field": ["1", "2", "3"], "int_field": 100}
            ),
        )

    def test_outside_limits_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestLimitsController.post(
                {
                    "key": "11",
                    "list_field": ["1", "2", "3", "4", "5"],
                    "int_field": 1000,
                }
            )
        self.assertEqual(
            {
                "int_field": ['Value "1000" is too big. Maximum value is 999.'],
                "key": ['Value "11" is too small. Minimum length is 3.'],
                "list_field": [
                    "['1', '2', '3', '4', '5'] contains too many values. Maximum length is 3."
                ],
            },
            cm.exception.errors,
        )
        self.assertEqual(
            {"int_field": 1000, "key": "11", "list_field": ["1", "2", "3", "4", "5"]},
            cm.exception.received_data,
        )

    def test_post_optional_missing_list_of_dict_is_valid(self):
        self.assertEqual(
            {"bool_field": False, "key": "my_key", "list_field": None},
            self.TestListController.post({"key": "my_key", "bool_field": False}),
        )

    def test_post_optional_list_of_dict_as_None_is_valid(self):
        self.assertEqual(
            {"bool_field": False, "key": "my_key", "list_field": None},
            self.TestListController.post(
                {"key": "my_key", "bool_field": False, "list_field": None}
            ),
        )

    def test_get_list_of_dict_is_valid(self):
        self.TestListController.post(
            {
                "key": "my_key",
                "list_field": [
                    {"first_key": EnumTest.Value1, "second_key": 1},
                    {"first_key": EnumTest.Value2, "second_key": 2},
                ],
                "bool_field": False,
            }
        )
        self.assertEqual(
            [
                {
                    "bool_field": False,
                    "key": "my_key",
                    "list_field": [
                        {"first_key": "Value1", "second_key": 1},
                        {"first_key": "Value2", "second_key": 2},
                    ],
                }
            ],
            self.TestListController.get(
                {
                    "list_field": [
                        {"first_key": EnumTest.Value1, "second_key": 1},
                        {"first_key": "Value2", "second_key": 2},
                    ]
                }
            ),
        )

    def test_get_optional_list_of_dict_as_None_is_skipped(self):
        self.TestListController.post(
            {
                "key": "my_key",
                "list_field": [
                    {"first_key": EnumTest.Value1, "second_key": 1},
                    {"first_key": EnumTest.Value2, "second_key": 2},
                ],
                "bool_field": False,
            }
        )
        self.assertEqual(
            [
                {
                    "bool_field": False,
                    "key": "my_key",
                    "list_field": [
                        {"first_key": "Value1", "second_key": 1},
                        {"first_key": "Value2", "second_key": 2},
                    ],
                }
            ],
            self.TestListController.get({"list_field": None}),
        )

    def test_delete_list_of_dict_is_valid(self):
        self.TestListController.post(
            {
                "key": "my_key",
                "list_field": [
                    {"first_key": EnumTest.Value1, "second_key": 1},
                    {"first_key": EnumTest.Value2, "second_key": 2},
                ],
                "bool_field": False,
            }
        )
        self.assertEqual(
            1,
            self.TestListController.delete(
                {
                    "list_field": [
                        {"first_key": EnumTest.Value1, "second_key": 1},
                        {"first_key": "Value2", "second_key": 2},
                    ]
                }
            ),
        )

    def test_delete_optional_list_of_dict_as_None_is_valid(self):
        self.TestListController.post(
            {
                "key": "my_key",
                "list_field": [
                    {"first_key": EnumTest.Value1, "second_key": 1},
                    {"first_key": EnumTest.Value2, "second_key": 2},
                ],
                "bool_field": False,
            }
        )
        self.assertEqual(1, self.TestListController.delete({"list_field": None}))

    def test_put_list_of_dict_is_valid(self):
        self.TestListController.post(
            {
                "key": "my_key",
                "list_field": [
                    {"first_key": EnumTest.Value1, "second_key": 1},
                    {"first_key": EnumTest.Value2, "second_key": 2},
                ],
                "bool_field": False,
            }
        )
        self.assertEqual(
            (
                {
                    "bool_field": False,
                    "key": "my_key",
                    "list_field": [
                        {"first_key": "Value1", "second_key": 1},
                        {"first_key": "Value2", "second_key": 2},
                    ],
                },
                {
                    "bool_field": True,
                    "key": "my_key",
                    "list_field": [
                        {"first_key": "Value2", "second_key": 10},
                        {"first_key": "Value1", "second_key": 2},
                    ],
                },
            ),
            self.TestListController.put(
                {
                    "key": "my_key",
                    "list_field": [
                        {"first_key": EnumTest.Value2, "second_key": 10},
                        {"first_key": EnumTest.Value1, "second_key": 2},
                    ],
                    "bool_field": True,
                }
            ),
        )

    def test_put_list_of_str_is_sorted(self):
        self.TestStringListController.post(
            {"key": "my_key", "list_field": ["a", "c", "b"]}
        )
        self.assertEqual(
            (
                {"key": "my_key", "list_field": ["a", "b", "c"]},
                {"key": "my_key", "list_field": ["d", "e", "f"]},
            ),
            self.TestStringListController.put(
                {"key": "my_key", "list_field": ["f", "e", "d"]}
            ),
        )

    def test_put_without_optional_list_of_dict_is_valid(self):
        self.TestListController.post(
            {
                "key": "my_key",
                "list_field": [
                    {"first_key": EnumTest.Value1, "second_key": 1},
                    {"first_key": EnumTest.Value2, "second_key": 2},
                ],
                "bool_field": False,
            }
        )
        self.assertEqual(
            (
                {
                    "bool_field": False,
                    "key": "my_key",
                    "list_field": [
                        {"first_key": "Value1", "second_key": 1},
                        {"first_key": "Value2", "second_key": 2},
                    ],
                },
                {
                    "bool_field": True,
                    "key": "my_key",
                    "list_field": [
                        {"first_key": "Value1", "second_key": 1},
                        {"first_key": "Value2", "second_key": 2},
                    ],
                },
            ),
            self.TestListController.put({"key": "my_key", "bool_field": True}),
        )

    def test_post_dict_is_valid(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )

    def test_post_missing_optional_dict_is_valid(self):
        self.assertEqual(
            {"dict_col": {"first_key": None, "second_key": None}, "key": "my_key"},
            self.TestOptionalDictController.post({"key": "my_key"}),
        )

    def test_post_optional_dict_as_None_is_valid(self):
        self.assertEqual(
            {"dict_col": {"first_key": None, "second_key": None}, "key": "my_key"},
            self.TestOptionalDictController.post({"key": "my_key", "dict_col": None}),
        )

    def test_put_missing_optional_dict_is_valid(self):
        self.TestOptionalDictController.post(
            {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
        )
        self.assertEqual(
            (
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
            ),
            self.TestOptionalDictController.put({"key": "my_key"}),
        )

    def test_post_empty_optional_dict_is_valid(self):
        self.assertEqual(
            {"key": "my_key", "dict_col": {}},
            self.TestOptionalDictController.post({"key": "my_key", "dict_col": {}}),
        )

    def test_put_empty_optional_dict_is_valid(self):
        self.TestOptionalDictController.post(
            {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
        )
        self.assertEqual(
            (
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
                {"key": "my_key", "dict_col": {}},
            ),
            self.TestOptionalDictController.put({"key": "my_key", "dict_col": {}}),
        )

    def test_put_optional_dict_as_None_is_valid(self):
        self.TestOptionalDictController.post(
            {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
        )
        self.assertEqual(
            (
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}},
            ),
            self.TestOptionalDictController.put({"key": "my_key", "dict_col": None}),
        )

    def test_get_optional_dict_as_None_is_valid(self):
        self.TestOptionalDictController.post(
            {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
        )
        self.assertEqual(
            [{"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}],
            self.TestOptionalDictController.get({"dict_col": None}),
        )

    def test_delete_optional_dict_as_None_is_valid(self):
        self.TestOptionalDictController.post(
            {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
        )
        self.assertEqual(1, self.TestOptionalDictController.delete({"dict_col": None}))

    def test_get_with_dot_notation_is_valid(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {
                    "key": "my_key",
                    "dict_col": {"first_key": EnumTest.Value1, "second_key": 3},
                }
            ),
        )
        self.assertEqual(
            [{"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}],
            self.TestDictController.get({"dict_col.first_key": EnumTest.Value1}),
        )

    def test_get_with_dot_notation_multi_level_is_valid(self):
        self.assertEqual(
            {
                "dict_field": {
                    "first_key": {"inner_key1": "Value1", "inner_key2": 3},
                    "second_key": 3,
                },
                "key": "my_key",
            },
            self.TestDictInDictController.post(
                {
                    "key": "my_key",
                    "dict_field": {
                        "first_key": {"inner_key1": EnumTest.Value1, "inner_key2": 3},
                        "second_key": 3,
                    },
                }
            ),
        )
        self.assertEqual(
            {
                "dict_field": {
                    "first_key": {"inner_key1": "Value2", "inner_key2": 3},
                    "second_key": 3,
                },
                "key": "my_key2",
            },
            self.TestDictInDictController.post(
                {
                    "key": "my_key2",
                    "dict_field": {
                        "first_key": {"inner_key1": EnumTest.Value2, "inner_key2": 3},
                        "second_key": 3,
                    },
                }
            ),
        )
        self.assertEqual(
            [
                {
                    "dict_field": {
                        "first_key": {"inner_key1": "Value1", "inner_key2": 3},
                        "second_key": 3,
                    },
                    "key": "my_key",
                }
            ],
            self.TestDictInDictController.get(
                {"dict_field.first_key.inner_key1": EnumTest.Value1}
            ),
        )

    def test_get_with_dot_notation_as_list_is_valid(self):
        self.TestDictController.post(
            {
                "key": "my_key",
                "dict_col": {"first_key": EnumTest.Value1, "second_key": 3},
            }
        )
        self.assertEqual(
            [{"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}],
            self.TestDictController.get({"dict_col.first_key": [EnumTest.Value1]}),
        )

    def test_get_with_multiple_results_dot_notation_as_list_is_valid(self):
        self.TestDictController.post_many(
            [
                {
                    "key": "my_key",
                    "dict_col": {"first_key": EnumTest.Value1, "second_key": 3},
                },
                {
                    "key": "my_key2",
                    "dict_col": {"first_key": EnumTest.Value2, "second_key": 4},
                },
            ]
        )
        self.assertEqual(
            [
                {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
                {
                    "dict_col": {"first_key": "Value2", "second_key": 4},
                    "key": "my_key2",
                },
            ],
            self.TestDictController.get(
                {"dict_col.first_key": [EnumTest.Value1, EnumTest.Value2]}
            ),
        )

    def test_update_with_dot_notation_is_valid(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )
        self.assertEqual(
            (
                {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
                {"dict_col": {"first_key": "Value1", "second_key": 4}, "key": "my_key"},
            ),
            self.TestDictController.put({"key": "my_key", "dict_col.second_key": 4}),
        )

    def test_update_with_dot_notation_invalid_value_is_invalid(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )
        with self.assertRaises(Exception) as cm:
            self.TestDictController.put(
                {"key": "my_key", "dict_col.second_key": "invalid integer"}
            )
        self.assertEqual(
            {"dict_col.second_key": ["Not a valid int."]}, cm.exception.errors
        )
        self.assertEqual(
            {"key": "my_key", "dict_col.second_key": "invalid integer"},
            cm.exception.received_data,
        )

    def test_delete_with_dot_notation_invalid_value_is_invalid(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )
        with self.assertRaises(Exception) as cm:
            self.TestDictController.delete({"dict_col.second_key": "invalid integer"})
        self.assertEqual(
            {"dict_col.second_key": ["Not a valid int."]}, cm.exception.errors
        )
        self.assertEqual(
            {"dict_col.second_key": "invalid integer"}, cm.exception.received_data
        )

    def test_delete_with_dot_notation_valid_value_is_valid(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )
        self.assertEqual(1, self.TestDictController.delete({"dict_col.second_key": 3}))

    def test_delete_with_dot_notation_enum_value_is_valid(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )
        self.assertEqual(
            1, self.TestDictController.delete({"dict_col.first_key": EnumTest.Value1})
        )

    def test_post_with_dot_notation_invalid_value_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestDictController.post(
                {
                    "key": "my_key",
                    "dict_col.first_key": "Value1",
                    "dict_col.second_key": "invalid integer",
                }
            )
        self.assertEqual(
            {"dict_col.second_key": ["Not a valid int."]}, cm.exception.errors
        )
        self.assertEqual(
            {
                "key": "my_key",
                "dict_col.first_key": "Value1",
                "dict_col.second_key": "invalid integer",
            },
            cm.exception.received_data,
        )

    def test_post_with_dot_notation_valid_value_is_valid(self):
        self.assertEqual(
            {"key": "my_key", "dict_col": {"first_key": "Value2", "second_key": 1}},
            self.TestDictController.post(
                {
                    "key": "my_key",
                    "dict_col.first_key": "Value2",
                    "dict_col.second_key": 1,
                }
            ),
        )

    def test_get_with_unmatching_dot_notation_is_empty(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )
        self.assertEqual(
            [], self.TestDictController.get({"dict_col.first_key": "Value2"})
        )

    def test_get_with_unknown_dot_notation_returns_everything(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )
        self.assertEqual(
            [{"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}],
            self.TestDictController.get({"dict_col.unknown": "Value1"}),
        )

    def test_delete_with_dot_notation_is_valid(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )
        self.assertEqual(
            1, self.TestDictController.delete({"dict_col.first_key": "Value1"})
        )
        self.assertEqual([], self.TestDictController.get({}))

    def test_delete_with_unmatching_dot_notation_is_empty(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )
        self.assertEqual(
            0, self.TestDictController.delete({"dict_col.first_key": "Value2"})
        )
        self.assertEqual(
            [{"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"}],
            self.TestDictController.get({}),
        )

    def test_delete_with_unknown_dot_notation_deletes_everything(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )
        self.assertEqual(
            1, self.TestDictController.delete({"dict_col.unknown": "Value2"})
        )
        self.assertEqual([], self.TestDictController.get({}))

    def test_put_without_primary_key_is_invalid(self):
        self.TestDictController.post(
            {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
        )
        with self.assertRaises(Exception) as cm:
            self.TestDictController.put(
                {"dict_col": {"first_key": "Value2", "second_key": 4}}
            )
        self.assertEqual(
            {"key": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual(
            {"dict_col": {"first_key": "Value2", "second_key": 4}},
            cm.exception.received_data,
        )

    def test_post_dict_with_dot_notation_is_valid(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {
                    "key": "my_key",
                    "dict_col.first_key": "Value1",
                    "dict_col.second_key": 3,
                }
            ),
        )

    def test_put_dict_with_dot_notation_is_valid(self):
        self.assertEqual(
            {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1", "second_key": 3}}
            ),
        )
        self.assertEqual(
            (
                {"dict_col": {"first_key": "Value1", "second_key": 3}, "key": "my_key"},
                {"dict_col": {"first_key": "Value2", "second_key": 3}, "key": "my_key"},
            ),
            self.TestDictController.put(
                {"key": "my_key", "dict_col.first_key": EnumTest.Value2}
            ),
        )

    def test_post_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestDictController.post(
                {"key": "my_key", "dict_col": {"first_key": "Value1"}}
            )
        self.assertEqual(
            {"dict_col.second_key": ["Missing data for required field."]},
            cm.exception.errors,
        )
        self.assertEqual(
            {"key": "my_key", "dict_col": {"first_key": "Value1"}},
            cm.exception.received_data,
        )

    def test_post_many_with_optional_is_valid(self):
        self.assertListEqual(
            [
                {"mandatory": 1, "key": "my_key", "optional": "my_value"},
                {"mandatory": 2, "key": "my_key2", "optional": "my_value2"},
            ],
            self.TestController.post_many(
                [
                    {"key": "my_key", "mandatory": 1, "optional": "my_value"},
                    {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
                ]
            ),
        )

    def test_post_with_unknown_field_is_valid(self):
        self.assertEqual(
            {"mandatory": 1, "key": "my_key", "optional": "my_value"},
            self.TestController.post(
                {
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": "my_value",
                    # This field do not exists in schema
                    "unknown": "my_value",
                }
            ),
        )

    def test_post_with_unknown_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestStrictController.post(
                {
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": "my_value",
                    # This field do not exists in schema
                    "unknown": "my_value",
                }
            )
        self.assertEqual({"unknown": ["Unknown field"]}, cm.exception.errors)
        self.assertEqual(
            {
                "key": "my_key",
                "mandatory": 1,
                "optional": "my_value",
                "unknown": "my_value",
            },
            cm.exception.received_data,
        )

    def test_post_many_with_unknown_field_is_valid(self):
        self.assertListEqual(
            [
                {"mandatory": 1, "key": "my_key", "optional": "my_value"},
                {"mandatory": 2, "key": "my_key2", "optional": "my_value2"},
            ],
            self.TestController.post_many(
                [
                    {
                        "key": "my_key",
                        "mandatory": 1,
                        "optional": "my_value",
                        # This field do not exists in schema
                        "unknown": "my_value",
                    },
                    {
                        "key": "my_key2",
                        "mandatory": 2,
                        "optional": "my_value2",
                        # This field do not exists in schema
                        "unknown": "my_value2",
                    },
                ]
            ),
        )

    def test_post_with_specified_incremented_field_is_ignored_and_valid(self):
        self.assertEqual(
            {"optional_with_default": "Test value", "key": 1, "enum_field": "Value1"},
            self.TestAutoIncrementController.post(
                {"key": "my_key", "enum_field": "Value1"}
            ),
        )

    def test_post_with_enum_is_valid(self):
        self.assertEqual(
            {"optional_with_default": "Test value", "key": 1, "enum_field": "Value1"},
            self.TestAutoIncrementController.post(
                {"key": "my_key", "enum_field": EnumTest.Value1}
            ),
        )

    def test_post_with_invalid_enum_choice_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestAutoIncrementController.post(
                {"key": "my_key", "enum_field": "InvalidValue"}
            )
        self.assertEqual(
            {
                "enum_field": [
                    "Value \"InvalidValue\" is not within ['Value1', 'Value2']."
                ]
            },
            cm.exception.errors,
        )
        self.assertEqual({"enum_field": "InvalidValue"}, cm.exception.received_data)

    def test_post_many_with_specified_incremented_field_is_ignored_and_valid(self):
        self.assertListEqual(
            [
                {
                    "optional_with_default": "Test value",
                    "enum_field": "Value1",
                    "key": 1,
                },
                {
                    "optional_with_default": "Test value",
                    "enum_field": "Value2",
                    "key": 2,
                },
            ],
            self.TestAutoIncrementController.post_many(
                [
                    {"key": "my_key", "enum_field": "Value1"},
                    {"key": "my_key", "enum_field": "Value2"},
                ]
            ),
        )

    def test_get_without_filter_is_retrieving_the_only_item(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.assertEqual(
            [{"mandatory": 1, "optional": "my_value1", "key": "my_key1"}],
            self.TestController.get({}),
        )

    def test_get_from_another_thread_than_post(self):
        def save_get_result():
            self.thread_get_result = self.TestController.get({})

        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )

        self.thread_get_result = None
        get_thread = Thread(name="GetInOtherThread", target=save_get_result)
        get_thread.start()
        get_thread.join()

        self.assertEqual(
            [{"mandatory": 1, "optional": "my_value1", "key": "my_key1"}],
            self.thread_get_result,
        )

    def test_get_without_filter_is_retrieving_everything_with_multiple_posts(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ],
            self.TestController.get({}),
        )

    def test_get_without_filter_is_retrieving_everything(self):
        self.TestController.post_many(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ]
        )
        self.assertEqual(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ],
            self.TestController.get({}),
        )

    def test_get_with_filter_is_retrieving_subset_with_multiple_posts(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(
            [{"key": "my_key1", "mandatory": 1, "optional": "my_value1"}],
            self.TestController.get({"optional": "my_value1"}),
        )

    def test_get_with_filter_is_retrieving_subset(self):
        self.TestController.post_many(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ]
        )
        self.assertEqual(
            [{"key": "my_key1", "mandatory": 1, "optional": "my_value1"}],
            self.TestController.get({"optional": "my_value1"}),
        )

    def test_put_is_updating(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.assertEqual(
            (
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
            ),
            self.TestController.put({"key": "my_key1", "optional": "my_value"}),
        )
        self.assertEqual(
            [{"key": "my_key1", "mandatory": 1, "optional": "my_value"}],
            self.TestController.get({"mandatory": 1}),
        )

    def test_put_is_updating_date(self):
        self.TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "2017-05-15",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
        self.assertEqual(
            (
                {
                    "date_str": "2017-05-15",
                    "datetime_str": "2016-09-23T23:59:59",
                    "key": "my_key1",
                },
                {
                    "date_str": "2018-06-01",
                    "datetime_str": "1989-12-31T01:00:00",
                    "key": "my_key1",
                },
            ),
            self.TestDateController.put(
                {
                    "key": "my_key1",
                    "date_str": "2018-06-01",
                    "datetime_str": "1989-12-31T01:00:00",
                }
            ),
        )
        self.assertEqual(
            [
                {
                    "date_str": "2018-06-01",
                    "datetime_str": "1989-12-31T01:00:00",
                    "key": "my_key1",
                }
            ],
            self.TestDateController.get({"date_str": "2018-06-01"}),
        )

    def test_get_date_is_handled_for_valid_date(self):
        self.TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "2017-05-15",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
        d = datetime.datetime.strptime("2017-05-15", "%Y-%m-%d").date()
        self.assertEqual(
            [
                {
                    "date_str": "2017-05-15",
                    "datetime_str": "2016-09-23T23:59:59",
                    "key": "my_key1",
                }
            ],
            self.TestDateController.get({"date_str": d}),
        )

    def test_post_invalid_date_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestDateController.post(
                {
                    "key": "my_key1",
                    "date_str": "this is not a date",
                    "datetime_str": "2016-09-23T23:59:59",
                }
            )
        self.assertEqual({"date_str": ["Not a valid date."]}, cm.exception.errors)
        self.assertEqual(
            {
                "key": "my_key1",
                "date_str": "this is not a date",
                "datetime_str": "2016-09-23T23:59:59",
            },
            cm.exception.received_data,
        )

    def test_get_invalid_date_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestDateController.get({"date_str": "this is not a date"})
        self.assertEqual({"date_str": ["Not a valid date."]}, cm.exception.errors)
        self.assertEqual({"date_str": "this is not a date"}, cm.exception.received_data)

    def test_delete_invalid_date_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestDateController.delete({"date_str": "this is not a date"})
        self.assertEqual({"date_str": ["Not a valid date."]}, cm.exception.errors)
        self.assertEqual({"date_str": "this is not a date"}, cm.exception.received_data)

    def test_get_with_unknown_fields_is_valid(self):
        self.TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "2018-12-30",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
        self.assertEqual(
            [
                {
                    "key": "my_key1",
                    "date_str": "2018-12-30",
                    "datetime_str": "2016-09-23T23:59:59",
                }
            ],
            self.TestDateController.get(
                {"date_str": "2018-12-30", "unknown_field": "value"}
            ),
        )

    def test_put_with_unknown_fields_is_valid(self):
        self.TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "2018-12-30",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
        self.assertEqual(
            (
                {
                    "key": "my_key1",
                    "date_str": "2018-12-30",
                    "datetime_str": "2016-09-23T23:59:59",
                },
                {
                    "key": "my_key1",
                    "date_str": "2018-12-31",
                    "datetime_str": "2016-09-23T23:59:59",
                },
            ),
            self.TestDateController.put(
                {"key": "my_key1", "date_str": "2018-12-31", "unknown_field": "value"}
            ),
        )
        self.assertEqual(
            [
                {
                    "key": "my_key1",
                    "date_str": "2018-12-31",
                    "datetime_str": "2016-09-23T23:59:59",
                }
            ],
            self.TestDateController.get({"date_str": "2018-12-31"}),
        )
        self.assertEqual([], self.TestDateController.get({"date_str": "2018-12-30"}))

    def test_put_unexisting_is_invalid(self):
        self.TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "2018-12-30",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
        with self.assertRaises(Exception) as cm:
            self.TestDateController.put({"key": "my_key2"})
        self.assertEqual({"key": "my_key2"}, cm.exception.requested_data)

    def test_post_invalid_datetime_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestDateController.post(
                {
                    "key": "my_key1",
                    "date_str": "2016-09-23",
                    "datetime_str": "This is not a valid datetime",
                }
            )
        self.assertEqual(
            {"datetime_str": ["Not a valid datetime."]}, cm.exception.errors
        )
        self.assertEqual(
            {
                "key": "my_key1",
                "date_str": "2016-09-23",
                "datetime_str": "This is not a valid datetime",
            },
            cm.exception.received_data,
        )

    def test_post_datetime_for_a_date_is_valid(self):
        self.assertEqual(
            {
                "key": "my_key1",
                "date_str": "2017-05-01",
                "datetime_str": "2017-05-30T01:05:45",
            },
            self.TestDateController.post(
                {
                    "key": "my_key1",
                    "date_str": datetime.datetime.strptime(
                        "2017-05-01T01:05:45", "%Y-%m-%dT%H:%M:%S"
                    ),
                    "datetime_str": "2017-05-30T01:05:45",
                }
            ),
        )

    def test_get_date_is_handled_for_unused_date(self):
        self.TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "2017-05-15",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
        d = datetime.datetime.strptime("2016-09-23", "%Y-%m-%d").date()
        self.assertEqual([], self.TestDateController.get({"date_str": d}))

    def test_get_date_is_handled_for_valid_datetime(self):
        self.TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "2017-05-15",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
        dt = datetime.datetime.strptime("2016-09-23T23:59:59", "%Y-%m-%dT%H:%M:%S")
        self.assertEqual(
            [
                {
                    "date_str": "2017-05-15",
                    "datetime_str": "2016-09-23T23:59:59",
                    "key": "my_key1",
                }
            ],
            self.TestDateController.get({"datetime_str": dt}),
        )

    def test_get_date_is_handled_for_unused_datetime(self):
        self.TestDateController.post(
            {
                "key": "my_key1",
                "date_str": "2017-05-15",
                "datetime_str": "2016-09-23T23:59:59",
            }
        )
        dt = datetime.datetime.strptime("2016-09-24T23:59:59", "%Y-%m-%dT%H:%M:%S")
        self.assertEqual([], self.TestDateController.get({"datetime_str": dt}))

    def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.put({"key": "my_key1", "optional": "my_value"})
        self.assertEqual([], self.TestController.get({"optional": "my_value1"}))

    def test_delete_with_filter_is_removing_the_proper_row(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(1, self.TestController.delete({"key": "my_key1"}))
        self.assertEqual(
            [{"key": "my_key2", "mandatory": 2, "optional": "my_value2"}],
            self.TestController.get({}),
        )

    def test_delete_without_filter_is_removing_everything(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(2, self.TestController.delete({}))
        self.assertEqual([], self.TestController.get({}))

    def test_query_get_parser(self):
        self.assertEqual(
            {
                "key": str,
                "mandatory": int,
                "optional": str,
                "limit": inputs.positive,
                "offset": inputs.natural,
            },
            parser_types(self.TestController.query_get_parser),
        )

    def test_query_get_parser_with_list_of_dict(self):
        self.assertEqual(
            {
                "bool_field": inputs.boolean,
                "key": str,
                "list_field": json.loads,
                "limit": inputs.positive,
                "offset": inputs.natural,
            },
            parser_types(self.TestListController.query_get_parser),
        )
        self.assertEqual(
            {
                "bool_field": "append",
                "key": "append",
                "list_field": "append",
                "limit": "store",
                "offset": "store",
            },
            parser_actions(self.TestListController.query_get_parser),
        )

    def test_query_get_parser_with_dict(self):
        self.assertEqual(
            {
                "dict_col.first_key": str,
                "dict_col.second_key": int,
                "key": str,
                "limit": inputs.positive,
                "offset": inputs.natural,
            },
            parser_types(self.TestDictController.query_get_parser),
        )
        self.assertEqual(
            {
                "dict_col.first_key": "append",
                "dict_col.second_key": "append",
                "key": "append",
                "limit": "store",
                "offset": "store",
            },
            parser_actions(self.TestDictController.query_get_parser),
        )

    def test_query_delete_parser(self):
        self.assertEqual(
            {"key": str, "mandatory": int, "optional": str},
            parser_types(self.TestController.query_delete_parser),
        )

    def test_query_delete_parser_with_list_of_dict(self):
        self.assertEqual(
            {"bool_field": inputs.boolean, "key": str, "list_field": json.loads},
            parser_types(self.TestListController.query_delete_parser),
        )
        self.assertEqual(
            {"bool_field": "append", "key": "append", "list_field": "append"},
            parser_actions(self.TestListController.query_delete_parser),
        )

    def test_query_rollback_parser(self):
        self.assertEqual(
            {
                "dict_field.first_key": str,
                "dict_field.second_key": int,
                "key": str,
                "revision": inputs.positive,
            },
            parser_types(self.TestVersionedController.query_rollback_parser),
        )
        self.assertEqual(
            {
                "dict_field.first_key": "append",
                "dict_field.second_key": "append",
                "key": "append",
                "revision": "store",
            },
            parser_actions(self.TestVersionedController.query_rollback_parser),
        )

    def test_query_delete_parser_with_dict(self):
        self.assertEqual(
            {"dict_col.first_key": str, "dict_col.second_key": int, "key": str},
            parser_types(self.TestDictController.query_delete_parser),
        )
        self.assertEqual(
            {
                "dict_col.first_key": "append",
                "dict_col.second_key": "append",
                "key": "append",
            },
            parser_actions(self.TestDictController.query_delete_parser),
        )

    def test_json_post_model(self):
        self.assertEqual("TestModel", self.TestController.json_post_model.name)
        self.assertEqual(
            {"key": "String", "mandatory": "Integer", "optional": "String"},
            self.TestController.json_post_model.fields_flask_type,
        )

    def test_json_post_model_with_auto_increment_and_enum(self):
        self.assertEqual(
            "TestAutoIncrementModel",
            self.TestAutoIncrementController.json_post_model.name,
        )
        self.assertEqual(
            {
                "enum_field": "String",
                "key": "Integer",
                "optional_with_default": "String",
            },
            self.TestAutoIncrementController.json_post_model.fields_flask_type,
        )
        self.assertEqual(
            {"enum_field": None, "key": None, "optional_with_default": "Test value"},
            self.TestAutoIncrementController.json_post_model.fields_default,
        )

    def test_json_put_model(self):
        self.assertEqual("TestModel", self.TestController.json_put_model.name)
        self.assertEqual(
            {"key": "String", "mandatory": "Integer", "optional": "String"},
            self.TestController.json_put_model.fields_flask_type,
        )

    def test_json_put_model_with_auto_increment_and_enum(self):
        self.assertEqual(
            "TestAutoIncrementModel",
            self.TestAutoIncrementController.json_put_model.name,
        )
        self.assertEqual(
            {
                "enum_field": "String",
                "key": "Integer",
                "optional_with_default": "String",
            },
            self.TestAutoIncrementController.json_put_model.fields_flask_type,
        )

    def test_get_response_model(self):
        self.assertEqual("TestModel", self.TestController.get_response_model.name)
        self.assertEqual(
            {"key": "String", "mandatory": "Integer", "optional": "String"},
            self.TestController.get_response_model.fields_flask_type,
        )

    def test_get_response_model_with_enum(self):
        self.assertEqual(
            "TestAutoIncrementModel",
            self.TestAutoIncrementController.get_response_model.name,
        )
        self.assertEqual(
            {
                "enum_field": "String",
                "key": "Integer",
                "optional_with_default": "String",
            },
            self.TestAutoIncrementController.get_response_model.fields_flask_type,
        )
        self.assertEqual(
            {
                "enum_field": "Test Documentation",
                "key": None,
                "optional_with_default": None,
            },
            self.TestAutoIncrementController.get_response_model.fields_description,
        )
        self.assertEqual(
            {
                "enum_field": ["Value1", "Value2"],
                "key": None,
                "optional_with_default": None,
            },
            self.TestAutoIncrementController.get_response_model.fields_enum,
        )

    def test_get_response_model_with_list_of_dict(self):
        self.assertEqual(
            "TestListModel", self.TestListController.get_response_model.name
        )
        self.assertEqual(
            {
                "bool_field": "Boolean",
                "key": "String",
                "list_field": (
                    "List",
                    {
                        "list_field_inner": (
                            "Nested",
                            {"first_key": "String", "second_key": "Integer"},
                        )
                    },
                ),
            },
            self.TestListController.get_response_model.fields_flask_type,
        )
        self.assertEqual(
            {
                "bool_field": None,
                "key": None,
                "list_field": (
                    None,
                    {
                        "list_field_inner": (
                            None,
                            {"first_key": None, "second_key": None},
                        )
                    },
                ),
            },
            self.TestListController.get_response_model.fields_description,
        )
        self.assertEqual(
            {
                "bool_field": None,
                "key": None,
                "list_field": (
                    None,
                    {
                        "list_field_inner": (
                            None,
                            {"first_key": ["Value1", "Value2"], "second_key": None},
                        )
                    },
                ),
            },
            self.TestListController.get_response_model.fields_enum,
        )
        self.assertEqual(
            {
                "bool_field": True,
                "key": "sample key",
                "list_field": (
                    [{"first_key": "Value1", "second_key": 1}],
                    {
                        "list_field_inner": (
                            {"first_key": "Value1", "second_key": 1},
                            {"first_key": "Value1", "second_key": 1},
                        )
                    },
                ),
            },
            self.TestListController.get_response_model.fields_example,
        )
        self.assertEqual(
            {
                "bool_field": None,
                "key": None,
                "list_field": (
                    None,
                    {
                        "list_field_inner": (
                            {"first_key": None, "second_key": None},
                            {"first_key": None, "second_key": None},
                        )
                    },
                ),
            },
            self.TestListController.get_response_model.fields_default,
        )
        self.assertEqual(
            {
                "bool_field": False,
                "key": False,
                "list_field": (
                    False,
                    {
                        "list_field_inner": (
                            False,
                            {"first_key": False, "second_key": False},
                        )
                    },
                ),
            },
            self.TestListController.get_response_model.fields_required,
        )
        self.assertEqual(
            {
                "bool_field": False,
                "key": False,
                "list_field": (
                    False,
                    {
                        "list_field_inner": (
                            False,
                            {"first_key": False, "second_key": False},
                        )
                    },
                ),
            },
            self.TestListController.get_response_model.fields_readonly,
        )

    def test_get_response_model_with_limits(self):
        self.assertEqual(
            "TestLimitsModel", self.TestLimitsController.get_response_model.name
        )
        self.assertEqual(
            {
                "int_field": "Integer",
                "key": "String",
                "list_field": ("List", {"list_field_inner": "String"}),
            },
            self.TestLimitsController.get_response_model.fields_flask_type,
        )
        self.assertEqual(
            {
                "int_field": None,
                "key": None,
                "list_field": (None, {"list_field_inner": None}),
            },
            self.TestLimitsController.get_response_model.fields_description,
        )
        self.assertEqual(
            {
                "int_field": None,
                "key": None,
                "list_field": (None, {"list_field_inner": None}),
            },
            self.TestLimitsController.get_response_model.fields_enum,
        )
        self.assertEqual(
            {
                "int_field": 100,
                "key": "XXX",
                "list_field": (["my", "test"], {"list_field_inner": None}),
            },
            self.TestLimitsController.get_response_model.fields_example,
        )
        self.assertEqual(
            {
                "int_field": None,
                "key": None,
                "list_field": (None, {"list_field_inner": None}),
            },
            self.TestLimitsController.get_response_model.fields_default,
        )
        self.assertEqual(
            {
                "int_field": False,
                "key": False,
                "list_field": (False, {"list_field_inner": None}),
            },
            self.TestLimitsController.get_response_model.fields_required,
        )
        self.assertEqual(
            {
                "int_field": False,
                "key": False,
                "list_field": (False, {"list_field_inner": None}),
            },
            self.TestLimitsController.get_response_model.fields_readonly,
        )

    def test_get_response_model_with_date(self):
        self.assertEqual(
            "TestDateModel", self.TestDateController.get_response_model.name
        )
        self.assertEqual(
            {"date_str": "Date", "datetime_str": "DateTime", "key": "String"},
            self.TestDateController.get_response_model.fields_flask_type,
        )
        self.assertEqual(
            {"date_str": None, "datetime_str": None, "key": None},
            self.TestDateController.get_response_model.fields_description,
        )
        self.assertEqual(
            {"date_str": None, "datetime_str": None, "key": None},
            self.TestDateController.get_response_model.fields_enum,
        )
        self.assertEqual(
            {
                "date_str": "2017-09-24",
                "datetime_str": "2017-09-24T15:36:09",
                "key": "sample key",
            },
            self.TestDateController.get_response_model.fields_example,
        )
        self.assertEqual(
            {"date_str": None, "datetime_str": None, "key": None},
            self.TestDateController.get_response_model.fields_default,
        )
        self.assertEqual(
            {"date_str": False, "datetime_str": False, "key": False},
            self.TestDateController.get_response_model.fields_required,
        )
        self.assertEqual(
            {"date_str": False, "datetime_str": False, "key": False},
            self.TestDateController.get_response_model.fields_readonly,
        )

    def test_get_response_model_with_float_and_unvalidated_list_and_dict(self):
        self.assertEqual(
            "TestUnvalidatedListAndDictModel",
            self.TestUnvalidatedListAndDictController.get_response_model.name,
        )
        self.assertEqual(
            {
                "dict_field": "Raw",
                "float_key": "Float",
                "float_with_default": "Float",
                "list_field": ("List", {"list_field_inner": "String"}),
            },
            self.TestUnvalidatedListAndDictController.get_response_model.fields_flask_type,
        )
        self.assertEqual(
            {
                "dict_field": None,
                "float_key": None,
                "float_with_default": None,
                "list_field": (None, {"list_field_inner": None}),
            },
            self.TestUnvalidatedListAndDictController.get_response_model.fields_description,
        )
        self.assertEqual(
            {
                "dict_field": None,
                "float_key": None,
                "float_with_default": None,
                "list_field": (None, {"list_field_inner": None}),
            },
            self.TestUnvalidatedListAndDictController.get_response_model.fields_enum,
        )
        self.assertEqual(
            {
                "dict_field": {
                    "1st dict_field key": "1st dict_field sample",
                    "2nd dict_field key": "2nd dict_field sample",
                },
                "float_key": 1.4,
                "float_with_default": 34,
                "list_field": (
                    ["1st list_field sample", "2nd list_field sample"],
                    {"list_field_inner": None},
                ),
            },
            self.TestUnvalidatedListAndDictController.get_response_model.fields_example,
        )
        self.assertEqual(
            {
                "dict_field": None,
                "float_key": None,
                "float_with_default": 34,
                "list_field": (None, {"list_field_inner": None}),
            },
            self.TestUnvalidatedListAndDictController.get_response_model.fields_default,
        )
        self.assertEqual(
            {
                "dict_field": True,
                "float_key": False,
                "float_with_default": False,
                "list_field": (True, {"list_field_inner": None}),
            },
            self.TestUnvalidatedListAndDictController.get_response_model.fields_required,
        )
        self.assertEqual(
            {
                "dict_field": False,
                "float_key": False,
                "float_with_default": False,
                "list_field": (False, {"list_field_inner": None}),
            },
            self.TestUnvalidatedListAndDictController.get_response_model.fields_readonly,
        )

    def test_post_float_as_int(self):
        self.assertEqual(
            {
                "dict_field": {"any_key": 5},
                "float_key": 1,
                "float_with_default": 34,
                "list_field": [22, "33", 44.55, True],
            },
            self.TestUnvalidatedListAndDictController.post(
                {
                    "dict_field": {"any_key": 5},
                    "float_key": 1,
                    "list_field": [22, "33", 44.55, True],
                }
            ),
        )

    def test_get_float_as_int(self):
        self.TestUnvalidatedListAndDictController.post(
            {
                "dict_field": {"any_key": 5},
                "float_key": 1,
                "list_field": [22, "33", 44.55, True],
            }
        )
        self.assertEqual(
            {
                "dict_field": {"any_key": 5},
                "float_key": 1,
                "float_with_default": 34,
                "list_field": [22, "33", 44.55, True],
            },
            self.TestUnvalidatedListAndDictController.get_one({"float_key": 1}),
        )

    def test_put_float_as_int(self):
        self.TestUnvalidatedListAndDictController.post(
            {
                "dict_field": {"any_key": 5},
                "float_key": 1,
                "list_field": [22, "33", 44.55, True],
            }
        )
        self.assertEqual(
            (
                {
                    "dict_field": {"any_key": 5},
                    "float_key": 1,
                    "float_with_default": 34,
                    "list_field": [22, "33", 44.55, True],
                },
                {
                    "dict_field": {"any_key": 6},
                    "float_key": 1,
                    "float_with_default": 35,
                    "list_field": [22, "33", 44.55, True],
                },
            ),
            self.TestUnvalidatedListAndDictController.put(
                {"dict_field.any_key": 6, "float_key": 1, "float_with_default": 35}
            ),
        )

    def test_get_with_limit_2_is_retrieving_subset_of_2_first_elements(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.TestController.post(
            {"key": "my_key3", "mandatory": 3, "optional": "my_value3"}
        )
        self.assertEqual(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ],
            self.TestController.get({"limit": 2}),
        )

    def test_get_with_offset_1_is_retrieving_subset_of_n_minus_1_first_elements(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.TestController.post(
            {"key": "my_key3", "mandatory": 3, "optional": "my_value3"}
        )
        self.assertEqual(
            [
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
                {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
            ],
            self.TestController.get({"offset": 1}),
        )

    def test_get_with_limit_1_and_offset_1_is_retrieving_middle_element(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.TestController.post(
            {"key": "my_key3", "mandatory": 3, "optional": "my_value3"}
        )
        self.assertEqual(
            [{"key": "my_key2", "mandatory": 2, "optional": "my_value2"}],
            self.TestController.get({"offset": 1, "limit": 1}),
        )

    def test_get_model_description_returns_description(self):
        self.assertEqual(
            {
                "key": "key",
                "mandatory": "mandatory",
                "optional": "optional",
                "collection": "sample_table_name",
            },
            self.TestController.get_model_description(),
        )

    def test_get_model_description_response_model(self):
        self.assertEqual(
            "TestModelDescription",
            self.TestController.get_model_description_response_model.name,
        )
        self.assertEqual(
            {
                "collection": "String",
                "key": "String",
                "mandatory": "String",
                "optional": "String",
            },
            self.TestController.get_model_description_response_model.fields_flask_type,
        )

    def test_get_is_valid_with_int_str_appended_by_less_than_sign_in_int_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"int_value": 122}, {"int_value": 123}, {"int_value": 124}])
        self.assertEqual(
            [
                {"int_value": 122, "float_value": None, "date_value": None, "datetime_value": None},
                {"int_value": 123, "float_value": None, "date_value": None, "datetime_value": None},
            ],
            self.TestSupportForComparisonSignsController.get_one({"int_value": "<124"}),
        )

    def test_get_is_valid_with_float_str_appended_by_less_than_sign_in_float_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"float_value": 0.9}, {"float_value": 1.0}, {"float_value": 1.1}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": 0.9, "date_value": None, "datetime_value": None},
                {"int_value": None, "float_value": 1.0, "date_value": None, "datetime_value": None},
            ],
            self.TestSupportForComparisonSignsController.get_one({"float_value": "<1.1"}),
        )

    def test_get_is_valid_with_date_str_appended_by_less_than_sign_in_date_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"date_value": "2019-01-01"}, {"date_value": "2019-01-02"}, {"date_value": "2019-01-03"}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": None, "date_value": "2019-01-01", "datetime_value": None},
                {"int_value": None, "float_value": None, "date_value": "2019-01-02", "datetime_value": None},
            ],
            self.TestSupportForComparisonSignsController.get_one({"date_value": "<2019-01-03"}),
        )

    def test_get_is_valid_with_datetime_str_appended_by_less_than_sign_in_datetime_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"datetime_value": "2019-01-01T23:59:59"}, {"datetime_value": "2019-01-02T23:59:59"}, {"datetime_value": "2019-01-03T23:59:59"}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": None, "date_value": None, "datetime_value": "2019-01-01T23:59:59"},
                {"int_value": None, "float_value": None, "date_value": None, "datetime_value": "2019-01-02T23:59:59"},
            ],
            self.TestSupportForComparisonSignsController.get_one({"int_value": "<2019-01-03T23:59:59"}),
        )

    def test_get_is_valid_with_int_str_appended_by_greater_than_sign_in_int_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"int_value": 122}, {"int_value": 123}, {"int_value": 124}])
        self.assertEqual(
            [
                {"int_value": 123, "float_value": None, "date_value": None, "datetime_value": None},
                {"int_value": 124, "float_value": None, "date_value": None, "datetime_value": None},
            ],
            self.TestSupportForComparisonSignsController.get_one({"int_value": ">122"}),
        )

    def test_get_is_valid_with_float_str_appended_by_greater_than_sign_in_float_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"float_value": 0.9}, {"float_value": 1.0}, {"float_value": 1.1}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": 1.0, "date_value": None, "datetime_value": None},
                {"int_value": None, "float_value": 1.1, "date_value": None, "datetime_value": None},
            ],
            self.TestSupportForComparisonSignsController.get_one({"float_value": ">0.9"}),
        )

    def test_get_is_valid_with_date_str_appended_by_greater_than_sign_in_date_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"date_value": "2019-01-01"}, {"date_value": "2019-01-02"}, {"date_value": "2019-01-03"}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": None, "date_value": "2019-01-02", "datetime_value": None},
                {"int_value": None, "float_value": None, "date_value": "2019-01-03", "datetime_value": None},
            ],
            self.TestSupportForComparisonSignsController.get_one({"date_value": ">2019-01-01"}),
        )

    def test_get_is_valid_with_datetime_str_appended_by_greater_than_sign_in_datetime_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"datetime_value": "2019-01-01T23:59:59"}, {"datetime_value": "2019-01-02T23:59:59"}, {"datetime_value": "2019-01-03T23:59:59"}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": None, "date_value": None, "datetime_value": "2019-01-02T23:59:59"},
                {"int_value": None, "float_value": None, "date_value": None, "datetime_value": "2019-01-03T23:59:59"},
            ],
            self.TestSupportForComparisonSignsController.get_one({"int_value": ">2019-01-01T23:59:59"}),
        )

    def test_get_is_valid_with_int_str_appended_by_less_than_or_equal_sign_in_int_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"int_value": 122}, {"int_value": 123}, {"int_value": 124}])
        self.assertEqual(
            [
                {"int_value": 122, "float_value": None, "date_value": None, "datetime_value": None},
                {"int_value": 123, "float_value": None, "date_value": None, "datetime_value": None},
                {"int_value": 124, "float_value": None, "date_value": None, "datetime_value": None},

            ],
            self.TestSupportForComparisonSignsController.get_one({"int_value": "<=124"}),
        )

    def test_get_is_valid_with_float_str_appended_by_less_than_or_equal_sign_in_float_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"float_value": 0.9}, {"float_value": 1.0}, {"float_value": 1.1}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": 0.9, "date_value": None, "datetime_value": None},
                {"int_value": None, "float_value": 1.0, "date_value": None, "datetime_value": None},
                {"int_value": None, "float_value": 1.1, "date_value": None, "datetime_value": None},
            ],
            self.TestSupportForComparisonSignsController.get_one({"float_value": "<=1.1"}),
        )

    def test_get_is_valid_with_date_str_appended_by_less_than_or_equal_sign_in_date_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"date_value": "2019-01-01"}, {"date_value": "2019-01-02"}, {"date_value": "2019-01-03"}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": None, "date_value": "2019-01-01", "datetime_value": None},
                {"int_value": None, "float_value": None, "date_value": "2019-01-02", "datetime_value": None},
                {"int_value": None, "float_value": None, "date_value": "2019-01-03", "datetime_value": None},
            ],
            self.TestSupportForComparisonSignsController.get_one({"date_value": "<=2019-01-03"}),
        )

    def test_get_is_valid_with_datetime_str_appended_by_less_than_or_equal_sign_in_datetime_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"datetime_value": "2019-01-01T23:59:59"}, {"datetime_value": "2019-01-02T23:59:59"}, {"datetime_value": "2019-01-03T23:59:59"}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": None, "date_value": None, "datetime_value": "2019-01-01T23:59:59"},
                {"int_value": None, "float_value": None, "date_value": None, "datetime_value": "2019-01-02T23:59:59"},
                {"int_value": None, "float_value": None, "date_value": None, "datetime_value": "2019-01-03T23:59:59"},
            ],
            self.TestSupportForComparisonSignsController.get_one({"int_value": "<=2019-01-03T23:59:59"}),
        )

    def test_get_is_valid_with_int_str_appended_by_greater_than_or_equal_sign_in_int_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"int_value": 122}, {"int_value": 123}, {"int_value": 124}])
        self.assertEqual(
            [
                {"int_value": 122, "float_value": None, "date_value": None, "datetime_value": None},
                {"int_value": 123, "float_value": None, "date_value": None, "datetime_value": None},
                {"int_value": 124, "float_value": None, "date_value": None, "datetime_value": None},
            ],
            self.TestSupportForComparisonSignsController.get_one({"int_value": ">=122"}),
        )

    def test_get_is_valid_with_float_str_appended_by_greater_than_or_equal_sign_in_float_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"float_value": 0.9}, {"float_value": 1.0}, {"float_value": 1.1}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": 0.9, "date_value": None, "datetime_value": None},
                {"int_value": None, "float_value": 1.0, "date_value": None, "datetime_value": None},
                {"int_value": None, "float_value": 1.1, "date_value": None, "datetime_value": None},
            ],
            self.TestSupportForComparisonSignsController.get_one({"float_value": ">=0.9"}),
        )

    def test_get_is_valid_with_date_str_appended_by_greater_than_or_equal_sign_in_date_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"date_value": "2019-01-01"}, {"date_value": "2019-01-02"}, {"date_value": "2019-01-03"}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": None, "date_value": "2019-01-01", "datetime_value": None},
                {"int_value": None, "float_value": None, "date_value": "2019-01-02", "datetime_value": None},
                {"int_value": None, "float_value": None, "date_value": "2019-01-03", "datetime_value": None},
            ],
            self.TestSupportForComparisonSignsController.get_one({"date_value": ">=2019-01-01"}),
        )

    def test_get_is_valid_with_datetime_str_appended_by_greater_than_or_equal_sign_in_datetime_column(self):
        self.TestSupportForComparisonSignsController.post_many([{"datetime_value": "2019-01-01T23:59:59"}, {"datetime_value": "2019-01-02T23:59:59"}, {"datetime_value": "2019-01-03T23:59:59"}])
        self.assertEqual(
            [
                {"int_value": None, "float_value": None, "date_value": None, "datetime_value": "2019-01-01T23:59:59"},
                {"int_value": None, "float_value": None, "date_value": None, "datetime_value": "2019-01-02T23:59:59"},
                {"int_value": None, "float_value": None, "date_value": None, "datetime_value": "2019-01-03T23:59:59"},
            ],
            self.TestSupportForComparisonSignsController.get_one({"int_value": ">=2019-01-01T23:59:59"}),
        )


class MongoCRUDControllerFailuresTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    _db = None

    @classmethod
    def setUpClass(cls):
        cls._db = database.load("mongomock", cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info("Declare model class...")

        class TestModel(
            database_mongo.CRUDModel, base=base, table_name="sample_table_name"
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            mandatory = database_mongo.Column(int, is_nullable=False)
            optional = database_mongo.Column(str)

        logger.info("Save model class...")
        return [TestModel]

    def setUp(self):
        logger.info(f"-------------------------------")
        logger.info(f"Start of {self._testMethodName}")
        database.reset(self._db)

    def tearDown(self):
        logger.info(f"End of {self._testMethodName}")
        logger.info(f"-------------------------------")

    def test_model_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.model(None)
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.",
        )

    def test_namespace_method_without_setting_model(self):
        class TestNamespace:
            pass

        with self.assertRaises(Exception) as cm:
            self.TestController.namespace(TestNamespace)
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.",
        )

    def test_counters_table_name_is_forbidden(self):
        with self.assertRaises(Exception) as cm:

            class TestModel(
                database_mongo.CRUDModel, base=self._db, table_name="counters"
            ):
                key = database_mongo.Column(str)

        self.assertEqual("counters is a reserved collection name.", str(cm.exception))

    def test_audit_table_name_is_forbidden(self):
        with self.assertRaises(Exception) as cm:

            class TestModel(
                database_mongo.CRUDModel, base=self._db, table_name="audit"
            ):
                key = database_mongo.Column(str)

        self.assertEqual("audit is a reserved collection name.", str(cm.exception))

    def test_audit_prefixed_table_name_is_forbidden(self):
        with self.assertRaises(Exception) as cm:

            class TestModel(
                database_mongo.CRUDModel, base=self._db, table_name="audit_toto"
            ):
                key = database_mongo.Column(str)

        self.assertEqual("audit_toto is a reserved collection name.", str(cm.exception))

    def test_get_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.get({})
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.",
        )

    def test_post_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({})
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.",
        )

    def test_post_many_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([])
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.",
        )

    def test_put_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.put({})
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.",
        )

    def test_delete_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.delete({})
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.",
        )

    def test_audit_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.get_audit({})
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.",
        )

    def test_model_description_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.get_model_description()
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.",
        )


class MongoCRUDControllerAuditTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    class TestEnumController(database.CRUDController):
        pass

    class TestVersionedController(database.CRUDController):
        pass

    class TestVersionedNoRollbackAllowedController(database.CRUDController):
        pass

    class TestPrimaryIntController(database.CRUDController):
        pass

    class TestIntController(database.CRUDController):
        pass

    class TestPrimaryIntVersionedController(database.CRUDController):
        pass

    class TestAutoIncAuditVersionedController(database.CRUDController):
        pass

    _db = None

    @classmethod
    def setUpClass(cls):
        cls._db = database.load(
            "mongomock?ssl=True", cls._create_models, replicaSet="globaldb"
        )
        cls.TestController.namespace(TestAPI)
        cls.TestEnumController.namespace(TestAPI)
        cls.TestVersionedController.namespace(TestAPI)
        cls.TestVersionedNoRollbackAllowedController.namespace(TestAPI)
        cls.TestPrimaryIntController.namespace(TestAPI)
        cls.TestIntController.namespace(TestAPI)
        cls.TestPrimaryIntVersionedController.namespace(TestAPI)
        cls.TestAutoIncAuditVersionedController.namespace(TestAPI)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info("Declare model class...")

        class TestModel(
            database_mongo.CRUDModel,
            base=base,
            table_name="sample_table_name",
            audit=True,
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            mandatory = database_mongo.Column(int, is_nullable=False)
            optional = database_mongo.Column(str)

        class TestEnumModel(
            database_mongo.CRUDModel,
            base=base,
            table_name="enum_table_name",
            audit=True,
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            enum_fld = database_mongo.Column(EnumTest)

        class TestPrimaryIntModel(
            database_mongo.CRUDModel,
            base=base,
            table_name="prim_int_table_name",
            audit=True,
        ):
            key = database_mongo.Column(
                int, is_primary_key=True, should_auto_increment=True
            )
            other = database_mongo.Column()

        class TestPrimaryIntVersionedModel(
            versioning_mongo.VersionedCRUDModel,
            base=base,
            table_name="prim_int_version_table_name",
            audit=True,
        ):
            key = database_mongo.Column(
                int, is_primary_key=True, should_auto_increment=True
            )
            other = database_mongo.Column()

        class TestAutoIncAuditVersionedModel(
            versioning_mongo.VersionedCRUDModel,
            base=base,
            table_name="prim_int_auto_inc_version_table_name",
            audit=True,
        ):
            key = database_mongo.Column(
                int, is_primary_key=True, should_auto_increment=True
            )
            other = database_mongo.Column(int)

        class TestIntModel(
            database_mongo.CRUDModel, base=base, table_name="int_table_name", audit=True
        ):
            key = database_mongo.Column(int)

        class TestVersionedModel(
            versioning_mongo.VersionedCRUDModel,
            base=base,
            table_name="versioned_table_name",
            audit=True,
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            enum_fld = database_mongo.Column(EnumTest)

        class TestVersionedNoRollbackAllowedModel(
            versioning_mongo.VersionedCRUDModel,
            base=base,
            table_name="versioned_no_rollback_table_name",
            audit=True,
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            enum_fld = database_mongo.Column(EnumTest)

            @classmethod
            def validate_rollback(
                cls, filters: dict, future_documents: List[dict]
            ) -> Dict[str, List[str]]:
                return {"key": ["Rollback forbidden"]}

        logger.info("Save model class...")
        cls.TestController.model(TestModel)
        cls.TestEnumController.model(TestEnumModel)
        cls.TestPrimaryIntController.model(TestPrimaryIntModel)
        cls.TestIntController.model(TestIntModel)
        cls.TestPrimaryIntVersionedController.model(TestPrimaryIntVersionedModel)
        cls.TestVersionedController.model(TestVersionedModel)
        cls.TestVersionedNoRollbackAllowedController.model(
            TestVersionedNoRollbackAllowedModel
        )
        cls.TestAutoIncAuditVersionedController.model(TestAutoIncAuditVersionedModel)
        return [
            TestModel,
            TestEnumModel,
            TestPrimaryIntModel,
            TestPrimaryIntVersionedModel,
            TestVersionedModel,
            TestIntModel,
            TestAutoIncAuditVersionedModel,
            TestVersionedNoRollbackAllowedModel,
        ]

    def setUp(self):
        logger.info(f"-------------------------------")
        logger.info(f"Start of {self._testMethodName}")
        database.reset(self._db)

    def tearDown(self):
        logger.info(f"End of {self._testMethodName}")
        logger.info(f"-------------------------------")

    def test_get_all_without_data_returns_empty_list(self):
        self.assertEqual([], self.TestController.get({}))
        self._check_audit(self.TestController, [])

    def test_audit_table_name_is_forbidden(self):
        with self.assertRaises(Exception) as cm:

            class TestAuditModel(
                database_mongo.CRUDModel, base=self._db, table_name="audit"
            ):
                key = database_mongo.Column(str)

        self.assertEqual("audit is a reserved collection name.", str(cm.exception))

    def test_audited_table_name_is_forbidden(self):
        with self.assertRaises(Exception) as cm:

            class TestAuditModel(
                database_mongo.CRUDModel,
                base=self._db,
                table_name="audit_int_table_name",
            ):
                key = database_mongo.Column(str)

        self.assertEqual(
            "audit_int_table_name is a reserved collection name.", str(cm.exception)
        )

    def test_get_parser_fields_order(self):
        self.assertEqual(
            {
                "key": str,
                "mandatory": int,
                "optional": str,
                "limit": inputs.positive,
                "offset": inputs.natural,
            },
            parser_types(self.TestController.query_get_parser),
        )

    def test_get_versioned_audit_parser_fields(self):
        self.assertEqual(
            {
                "audit_action": str,
                "audit_date_utc": inputs.datetime_from_iso8601,
                "audit_user": str,
                "limit": inputs.positive,
                "offset": inputs.natural,
                "revision": int,
            },
            parser_types(self.TestVersionedController.query_get_audit_parser),
        )

    def test_delete_parser_fields_order(self):
        self.assertEqual(
            {"key": str, "mandatory": int, "optional": str},
            parser_types(self.TestController.query_delete_parser),
        )

    def test_post_model_fields_order(self):
        self.assertEqual(
            {"key": "String", "mandatory": "Integer", "optional": "String"},
            self.TestController.json_post_model.fields_flask_type,
        )

    def test_versioned_audit_get_response_model_fields(self):
        self.assertEqual(
            {
                "audit_action": "String",
                "audit_date_utc": "DateTime",
                "audit_user": "String",
                "revision": "Integer",
            },
            self.TestVersionedController.get_audit_response_model.fields_flask_type,
        )

    def test_put_model_fields_order(self):
        self.assertEqual(
            {"key": "String", "mandatory": "Integer", "optional": "String"},
            self.TestController.json_put_model.fields_flask_type,
        )

    def test_get_response_model_fields_order(self):
        self.assertEqual(
            {"key": "String", "mandatory": "Integer", "optional": "String"},
            self.TestController.get_response_model.fields_flask_type,
        )

    def test_post_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual(None, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_post_many_with_nothing_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.post_many(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual([], cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_post_with_empty_dict_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.post({})
        self.assertEqual(
            {
                "key": ["Missing data for required field."],
                "mandatory": ["Missing data for required field."],
            },
            cm.exception.errors,
        )
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_post_many_with_empty_list_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.post_many([])
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual([], cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_put_with_nothing_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.put(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual(None, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_put_with_empty_dict_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.put({})
        self.assertEqual(
            {"key": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_delete_without_nothing_do_not_fail(self):
        self.assertEqual(0, self.TestController.delete({}))
        self._check_audit(self.TestController, [])

    def test_post_without_mandatory_field_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self.TestController.post({"key": "my_key"})
        self.assertEqual(
            {"mandatory": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual({"key": "my_key"}, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_post_many_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{"key": "my_key"}])
        self.assertEqual(
            {0: {"mandatory": ["Missing data for required field."]}},
            cm.exception.errors,
        )
        self.assertEqual([{"key": "my_key"}], cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_post_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({"mandatory": 1})
        self.assertEqual(
            {"key": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual({"mandatory": 1}, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_post_many_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{"mandatory": 1}])
        self.assertEqual(
            {0: {"key": ["Missing data for required field."]}}, cm.exception.errors
        )
        self.assertEqual([{"mandatory": 1}], cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_post_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post(
                {"key": datetime.date(2007, 12, 5), "mandatory": 1}
            )
        self.assertEqual({"key": ["Not a valid str."]}, cm.exception.errors)
        self.assertEqual(
            {"key": datetime.date(2007, 12, 5), "mandatory": 1},
            cm.exception.received_data,
        )
        self._check_audit(self.TestController, [])

    def test_post_many_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many(
                [{"key": datetime.date(2007, 12, 5), "mandatory": 1}]
            )
        self.assertEqual({0: {"key": ["Not a valid str."]}}, cm.exception.errors)
        self.assertEqual(
            [{"key": datetime.date(2007, 12, 5), "mandatory": 1}],
            cm.exception.received_data,
        )
        self._check_audit(self.TestController, [])

    def test_put_with_wrong_type_is_invalid(self):
        self.TestController.post({"key": "value1", "mandatory": 1})
        with self.assertRaises(Exception) as cm:
            self.TestController.put({"key": "value1", "mandatory": "invalid_value"})
        self.assertEqual({"mandatory": ["Not a valid int."]}, cm.exception.errors)
        self.assertEqual(
            {"key": "value1", "mandatory": "invalid_value"}, cm.exception.received_data
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "value1",
                    "mandatory": 1,
                    "optional": None,
                    "revision": 1,
                }
            ],
        )

    def test_versioned_int_primary_key_is_reset_after_delete(self):
        self.assertEqual(
            {
                "key": 1,
                "other": "test1",
                "valid_since_revision": 1,
                "valid_until_revision": -1,
            },
            self.TestPrimaryIntVersionedController.post({"other": "test1"}),
        )
        self.assertEqual(1, self.TestPrimaryIntVersionedController.delete({}))
        self.assertEqual(
            {
                "key": 1,
                "other": "test1",
                "valid_since_revision": 3,
                "valid_until_revision": -1,
            },
            self.TestPrimaryIntVersionedController.post({"other": "test1"}),
        )
        self._check_audit(
            self.TestPrimaryIntVersionedController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "revision": 1,
                    "table_name": "prim_int_version_table_name",
                },
                {
                    "audit_action": "Delete",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "revision": 2,
                    "table_name": "prim_int_version_table_name",
                },
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "revision": 3,
                    "table_name": "prim_int_version_table_name",
                },
            ],
        )
        self.assertEqual(
            [
                {
                    "key": 1,
                    "other": "test1",
                    "valid_since_revision": 1,
                    "valid_until_revision": 2,
                },
                {
                    "key": 1,
                    "other": "test1",
                    "valid_since_revision": 3,
                    "valid_until_revision": -1,
                },
            ],
            self.TestPrimaryIntVersionedController.get_history({}),
        )

    def test_auto_incremented_fields_are_not_incremented_on_post_failure(self):
        self.assertEqual(
            {
                "key": 1,
                "other": 1,
                "valid_since_revision": 1,
                "valid_until_revision": -1,
            },
            self.TestAutoIncAuditVersionedController.post({"other": 1}),
        )

        # Should not increment revision, nor the auto incremented key
        with self.assertRaises(ValidationFailed):
            self.TestAutoIncAuditVersionedController.post({"other": "FAILED"})

        self.assertEqual(
            {
                "key": 2,
                "other": 2,
                "valid_since_revision": 2,
                "valid_until_revision": -1,
            },
            self.TestAutoIncAuditVersionedController.post({"other": 2}),
        )

    def test_auto_incremented_fields_are_not_incremented_on_multi_post_failure(self):
        self.assertEqual(
            [
                {
                    "key": 1,
                    "other": 1,
                    "valid_since_revision": 1,
                    "valid_until_revision": -1,
                }
            ],
            self.TestAutoIncAuditVersionedController.post_many([{"other": 1}]),
        )

        # Should not increment revision, nor the auto incremented key
        with self.assertRaises(ValidationFailed):
            self.TestAutoIncAuditVersionedController.post_many(
                [{"other": 2}, {"other": "FAILED"}, {"other": 4}]
            )

        self.assertEqual(
            [
                {
                    "key": 3,  # For performance reasons, deserialization is performed before checks on other doc (so first valid document incremented the counter)
                    "other": 5,
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                }
            ],
            self.TestAutoIncAuditVersionedController.post_many([{"other": 5}]),
        )

    def test_auto_incremented_fields_are_not_incremented_on_multi_post_failure(self):
        self.assertEqual(
            [
                {
                    "key": 1,
                    "other": 1,
                    "valid_since_revision": 1,
                    "valid_until_revision": -1,
                }
            ],
            self.TestAutoIncAuditVersionedController.post_many([{"other": 1}]),
        )

        # Should not increment revision
        with self.assertRaises(ValidationFailed):
            self.TestAutoIncAuditVersionedController.put_many(
                [{"other": 1}, {"other": "FAILED"}, {"other": 1}]
            )

        self.assertEqual(
            [
                {
                    "key": 2,
                    "other": 5,
                    "valid_since_revision": 2,
                    "valid_until_revision": -1,
                }
            ],
            self.TestAutoIncAuditVersionedController.post_many([{"other": 5}]),
        )

    def test_int_primary_key_is_reset_after_delete(self):
        self.assertEqual(
            {"key": 1, "other": "test1"},
            self.TestPrimaryIntController.post({"other": "test1"}),
        )
        self.assertEqual(1, self.TestPrimaryIntController.delete({}))
        self.assertEqual(
            {"key": 1, "other": "test1"},
            self.TestPrimaryIntController.post({"other": "test1"}),
        )
        self.assertEqual(
            {"key": 2, "other": "test1"},
            self.TestPrimaryIntController.post({"other": "test1"}),
        )
        self._check_audit(
            self.TestPrimaryIntController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": 1,
                    "other": "test1",
                    "revision": 1,
                },
                {
                    "audit_action": "Delete",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": 1,
                    "other": "test1",
                    "revision": 2,
                },
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": 1,
                    "other": "test1",
                    "revision": 3,
                },
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": 2,
                    "other": "test1",
                    "revision": 4,
                },
            ],
        )

    def test_int_revision_is_not_reset_after_delete(self):
        self.assertEqual({"key": 1}, self.TestIntController.post({"key": 1}))
        self.assertEqual(1, self.TestIntController.delete({}))
        self.assertEqual({"key": 1}, self.TestIntController.post({"key": 1}))
        self.assertEqual({"key": 2}, self.TestIntController.post({"key": 2}))
        self._check_audit(
            self.TestIntController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": 1,
                    "revision": 1,
                },
                {
                    "audit_action": "Delete",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": 1,
                    "revision": 2,
                },
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": 1,
                    "revision": 3,
                },
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": 2,
                    "revision": 4,
                },
            ],
        )

    def test_post_without_optional_is_valid(self):
        self.assertEqual(
            {"optional": None, "mandatory": 1, "key": "my_key"},
            self.TestController.post({"key": "my_key", "mandatory": 1}),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": None,
                    "revision": 1,
                }
            ],
        )

    def test_revision_not_shared_if_not_versioned(self):
        self.assertEqual(
            {"optional": None, "mandatory": 1, "key": "my_key"},
            self.TestController.post({"key": "my_key", "mandatory": 1}),
        )
        self.TestVersionedController.post(
            {"key": "my_key", "enum_fld": EnumTest.Value1}
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": None,
                    "revision": 1,
                }
            ],
        )
        self._check_audit(
            self.TestVersionedController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "revision": 1,
                    "table_name": "versioned_table_name",
                }
            ],
        )

    def test_post_with_enum_is_valid(self):
        self.assertEqual(
            {"enum_fld": "Value1", "key": "my_key"},
            self.TestEnumController.post(
                {"key": "my_key", "enum_fld": EnumTest.Value1}
            ),
        )
        self._check_audit(
            self.TestEnumController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "enum_fld": "Value1",
                    "key": "my_key",
                    "revision": 1,
                }
            ],
        )

    def test_put_with_enum_is_valid(self):
        self.assertEqual(
            {"enum_fld": "Value1", "key": "my_key"},
            self.TestEnumController.post(
                {"key": "my_key", "enum_fld": EnumTest.Value1}
            ),
        )
        self.assertEqual(
            (
                {"enum_fld": "Value1", "key": "my_key"},
                {"enum_fld": "Value2", "key": "my_key"},
            ),
            self.TestEnumController.put({"key": "my_key", "enum_fld": EnumTest.Value2}),
        )
        self._check_audit(
            self.TestEnumController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "enum_fld": "Value1",
                    "key": "my_key",
                    "revision": 1,
                },
                {
                    "audit_action": "Update",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "enum_fld": "Value2",
                    "key": "my_key",
                    "revision": 2,
                },
            ],
        )

    def test_revision_on_versionned_audit_after_put_failure(self):
        self.TestVersionedController.post(
            {"key": "my_key", "enum_fld": EnumTest.Value1}
        )
        with self.assertRaises(ModelCouldNotBeFound):
            self.TestVersionedController.put(
                {"key": "my_key2", "enum_fld": EnumTest.Value2}
            )
        self.TestVersionedController.delete({"key": "my_key"})
        self._check_audit(
            self.TestVersionedController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "revision": 1,
                    "table_name": "versioned_table_name",
                },
                {
                    "audit_action": "Delete",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "revision": 2,
                    "table_name": "versioned_table_name",
                },
            ],
        )

    def test_versioned_audit_after_post_put_delete_rollback(self):
        self.TestVersionedController.post(
            {"key": "my_key", "enum_fld": EnumTest.Value1}
        )
        self.TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
        self.TestVersionedController.delete({"key": "my_key"})
        self.TestVersionedController.rollback_to({"revision": 1})
        self._check_audit(
            self.TestVersionedController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "revision": 1,
                    "table_name": "versioned_table_name",
                },
                {
                    "audit_action": "Update",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "revision": 2,
                    "table_name": "versioned_table_name",
                },
                {
                    "audit_action": "Delete",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "revision": 3,
                    "table_name": "versioned_table_name",
                },
                {
                    "audit_action": "Rollback",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "revision": 4,
                    "table_name": "versioned_table_name",
                },
            ],
        )

    def test_rollback_validation_custom(self):
        self.TestVersionedNoRollbackAllowedController.post(
            {"key": "my_key", "enum_fld": EnumTest.Value1}
        )
        self.TestVersionedNoRollbackAllowedController.put(
            {"key": "my_key", "enum_fld": EnumTest.Value2}
        )
        self.TestVersionedNoRollbackAllowedController.delete({"key": "my_key"})
        with self.assertRaises(ValidationFailed) as cm:
            self.TestVersionedNoRollbackAllowedController.rollback_to({"revision": 1})
        self.assertEqual({"key": ["Rollback forbidden"]}, cm.exception.errors)
        self.assertEqual({"revision": 1}, cm.exception.received_data)

    def test_get_last_when_empty(self):
        self.assertEqual(self.TestVersionedController.get_last({}), {})

    def test_get_last_when_single_doc_post(self):
        self.TestVersionedController.post(
            {"key": "my_key", "enum_fld": EnumTest.Value1}
        )
        self.assertEqual(
            self.TestVersionedController.get_last({}),
            {
                "enum_fld": "Value1",
                "key": "my_key",
                "valid_since_revision": 1,
                "valid_until_revision": -1,
            },
        )

    def test_get_last_with_unmatched_filter(self):
        self.TestVersionedController.post(
            {"key": "my_key", "enum_fld": EnumTest.Value1}
        )
        self.assertEqual(self.TestVersionedController.get_last({"key": "my_key2"}), {})

    def test_get_last_when_single_update(self):
        self.TestVersionedController.post(
            {"key": "my_key", "enum_fld": EnumTest.Value1}
        )
        self.TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
        self.assertEqual(
            self.TestVersionedController.get_last({}),
            {
                "enum_fld": "Value2",
                "key": "my_key",
                "valid_since_revision": 2,
                "valid_until_revision": -1,
            },
        )

    def test_get_last_when_removed(self):
        self.TestVersionedController.post(
            {"key": "my_key", "enum_fld": EnumTest.Value1}
        )
        self.TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
        self.TestVersionedController.delete({"key": "my_key"})
        self.assertEqual(
            self.TestVersionedController.get_last({}),
            {
                "enum_fld": "Value2",
                "key": "my_key",
                "valid_since_revision": 2,
                "valid_until_revision": 3,
            },
        )

    def test_get_last_with_one_removed_and_a_valid(self):
        self.TestVersionedController.post(
            {"key": "my_key", "enum_fld": EnumTest.Value1}
        )
        self.TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
        self.TestVersionedController.delete({"key": "my_key"})
        self.TestVersionedController.post(
            {"key": "my_key2", "enum_fld": EnumTest.Value1}
        )
        self.assertEqual(
            self.TestVersionedController.get_last({}),
            {
                "enum_fld": "Value1",
                "key": "my_key2",
                "valid_since_revision": 4,
                "valid_until_revision": -1,
            },
        )

    def test_get_last_with_one_removed_and_a_valid_and_filter_on_removed(self):
        self.TestVersionedController.post(
            {"key": "my_key", "enum_fld": EnumTest.Value1}
        )
        self.TestVersionedController.put({"key": "my_key", "enum_fld": EnumTest.Value2})
        self.TestVersionedController.delete({"key": "my_key"})
        self.TestVersionedController.post(
            {"key": "my_key2", "enum_fld": EnumTest.Value1}
        )
        self.assertEqual(
            self.TestVersionedController.get_last({"key": "my_key"}),
            {
                "enum_fld": "Value2",
                "key": "my_key",
                "valid_since_revision": 2,
                "valid_until_revision": 3,
            },
        )

    def test_delete_with_enum_is_valid(self):
        self.assertEqual(
            {"enum_fld": "Value1", "key": "my_key"},
            self.TestEnumController.post(
                {"key": "my_key", "enum_fld": EnumTest.Value1}
            ),
        )
        self.assertEqual(
            1, self.TestEnumController.delete({"enum_fld": EnumTest.Value1})
        )
        self._check_audit(
            self.TestEnumController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "enum_fld": "Value1",
                    "key": "my_key",
                    "revision": 1,
                },
                {
                    "audit_action": "Delete",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "enum_fld": "Value1",
                    "key": "my_key",
                    "revision": 2,
                },
            ],
        )

    def test_post_many_without_optional_is_valid(self):
        self.assertListEqual(
            [{"optional": None, "mandatory": 1, "key": "my_key"}],
            self.TestController.post_many([{"key": "my_key", "mandatory": 1}]),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": None,
                    "revision": 1,
                }
            ],
        )

    def test_put_many_is_valid(self):
        self.TestController.post_many(
            [{"key": "my_key", "mandatory": 1}, {"key": "my_key2", "mandatory": 2}]
        )
        self.TestController.put_many(
            [{"key": "my_key", "optional": "test"}, {"key": "my_key2", "mandatory": 3}]
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": None,
                    "revision": 1,
                },
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key2",
                    "mandatory": 2,
                    "optional": None,
                    "revision": 2,
                },
                {
                    "audit_action": "Update",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": "test",
                    "revision": 3,
                },
                {
                    "audit_action": "Update",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key2",
                    "mandatory": 3,
                    "optional": None,
                    "revision": 4,
                },
            ],
        )

    def _check_audit(self, controller, expected_audit, filter_audit={}):
        audit = controller.get_audit(filter_audit)
        audit = [
            {key: audit_line[key] for key in sorted(audit_line.keys())}
            for audit_line in audit
        ]

        if not expected_audit:
            self.assertEqual(audit, expected_audit)
        else:
            self.assertRegex(
                f"{audit}",
                f"{expected_audit}".replace("[", "\\[")
                .replace("]", "\\]")
                .replace("\\\\", "\\"),
            )

    def test_post_with_optional_is_valid(self):
        self.assertEqual(
            {"mandatory": 1, "key": "my_key", "optional": "my_value"},
            self.TestController.post(
                {"key": "my_key", "mandatory": 1, "optional": "my_value"}
            ),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": "my_value",
                    "revision": 1,
                }
            ],
        )

    def test_post_many_with_optional_is_valid(self):
        self.assertListEqual(
            [{"mandatory": 1, "key": "my_key", "optional": "my_value"}],
            self.TestController.post_many(
                [{"key": "my_key", "mandatory": 1, "optional": "my_value"}]
            ),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": "my_value",
                    "revision": 1,
                }
            ],
        )

    def test_post_with_unknown_field_is_valid(self):
        self.assertEqual(
            {"optional": "my_value", "mandatory": 1, "key": "my_key"},
            self.TestController.post(
                {
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": "my_value",
                    # This field do not exists in schema
                    "unknown": "my_value",
                }
            ),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": "my_value",
                    "revision": 1,
                }
            ],
        )

    def test_post_many_with_unknown_field_is_valid(self):
        self.assertListEqual(
            [{"optional": "my_value", "mandatory": 1, "key": "my_key"}],
            self.TestController.post_many(
                [
                    {
                        "key": "my_key",
                        "mandatory": 1,
                        "optional": "my_value",
                        # This field do not exists in schema
                        "unknown": "my_value",
                    }
                ]
            ),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": "my_value",
                    "revision": 1,
                }
            ],
        )

    def test_get_without_filter_is_retrieving_the_only_item(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.assertEqual(
            [{"mandatory": 1, "optional": "my_value1", "key": "my_key1"}],
            self.TestController.get({}),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                }
            ],
        )

    def test_get_without_filter_is_retrieving_everything_with_multiple_posts(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ],
            self.TestController.get({}),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key2",
                    "mandatory": 2,
                    "optional": "my_value2",
                    "revision": 2,
                },
            ],
        )

    def test_get_without_filter_is_retrieving_everything(self):
        self.TestController.post_many(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ]
        )
        self.assertEqual(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ],
            self.TestController.get({}),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key2",
                    "mandatory": 2,
                    "optional": "my_value2",
                    "revision": 2,
                },
            ],
        )

    def test_get_with_filter_is_retrieving_subset(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(
            [{"key": "my_key1", "mandatory": 1, "optional": "my_value1"}],
            self.TestController.get({"optional": "my_value1"}),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key2",
                    "mandatory": 2,
                    "optional": "my_value2",
                    "revision": 2,
                },
            ],
        )

    def test_put_is_updating(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.assertEqual(
            (
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
            ),
            self.TestController.put({"key": "my_key1", "optional": "my_value"}),
        )
        self.assertEqual(
            [{"key": "my_key1", "mandatory": 1, "optional": "my_value"}],
            self.TestController.get({"mandatory": 1}),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "Update",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value",
                    "revision": 2,
                },
            ],
        )

    def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.put({"key": "my_key1", "optional": "my_value"})
        self.assertEqual([], self.TestController.get({"optional": "my_value1"}))
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "Update",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value",
                    "revision": 2,
                },
            ],
        )

    def test_delete_with_filter_is_removing_the_proper_row(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(1, self.TestController.delete({"key": "my_key1"}))
        self.assertEqual(
            [{"key": "my_key2", "mandatory": 2, "optional": "my_value2"}],
            self.TestController.get({}),
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key2",
                    "mandatory": 2,
                    "optional": "my_value2",
                    "revision": 2,
                },
                {
                    "audit_action": "Delete",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 3,
                },
            ],
        )

    def test_audit_filter_on_model_is_returning_only_selected_data(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.put({"key": "my_key1", "mandatory": 2})
        self.TestController.delete({"key": "my_key1"})
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "Update",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 2,
                    "optional": "my_value1",
                    "revision": 2,
                },
                {
                    "audit_action": "Delete",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 2,
                    "optional": "my_value1",
                    "revision": 3,
                },
            ],
            filter_audit={"key": "my_key1"},
        )

    def test_audit_filter_on_audit_model_is_returning_only_selected_data(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.put({"key": "my_key1", "mandatory": 2})
        self.TestController.delete({"key": "my_key1"})
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Update",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 2,
                    "optional": "my_value1",
                    "revision": 2,
                }
            ],
            filter_audit={"audit_action": "Update"},
        )

    def test_value_can_be_updated_to_previous_value(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.put({"key": "my_key1", "mandatory": 2})
        self.TestController.put(
            {"key": "my_key1", "mandatory": 1}  # Put back initial value
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "Update",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 2,
                    "optional": "my_value1",
                    "revision": 2,
                },
                {
                    "audit_action": "Update",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 3,
                },
            ],
        )

    def test_delete_without_filter_is_removing_everything(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(2, self.TestController.delete({}))
        self.assertEqual([], self.TestController.get({}))
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "Insert",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key2",
                    "mandatory": 2,
                    "optional": "my_value2",
                    "revision": 2,
                },
                {
                    "audit_action": "Delete",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 3,
                },
                {
                    "audit_action": "Delete",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.)?(\d{0,6})",
                    "audit_user": "",
                    "key": "my_key2",
                    "mandatory": 2,
                    "optional": "my_value2",
                    "revision": 4,
                },
            ],
        )

    def test_query_get_parser(self):
        self.assertEqual(
            {
                "key": str,
                "mandatory": int,
                "optional": str,
                "limit": inputs.positive,
                "offset": inputs.natural,
            },
            parser_types(self.TestController.query_get_parser),
        )
        self._check_audit(self.TestController, [])

    def test_query_get_audit_parser(self):
        self.assertEqual(
            {
                "audit_action": str,
                "audit_date_utc": inputs.datetime_from_iso8601,
                "audit_user": str,
                "key": str,
                "mandatory": int,
                "optional": str,
                "limit": inputs.positive,
                "offset": inputs.natural,
                "revision": int,
            },
            parser_types(self.TestController.query_get_audit_parser),
        )
        self._check_audit(self.TestController, [])

    def test_query_delete_parser(self):
        self.assertEqual(
            {"key": str, "mandatory": int, "optional": str},
            parser_types(self.TestController.query_delete_parser),
        )
        self._check_audit(self.TestController, [])

    def test_get_response_model(self):
        self.assertEqual("TestModel", self.TestController.get_response_model.name)
        self.assertEqual(
            {"key": "String", "mandatory": "Integer", "optional": "String"},
            self.TestController.get_response_model.fields_flask_type,
        )
        self._check_audit(self.TestController, [])

    def test_get_audit_response_model(self):
        self.assertEqual(
            "AuditTestModel", self.TestController.get_audit_response_model.name
        )
        self.assertEqual(
            {
                "audit_action": "String",
                "audit_date_utc": "DateTime",
                "audit_user": "String",
                "key": "String",
                "mandatory": "Integer",
                "optional": "String",
                "revision": "Integer",
            },
            self.TestController.get_audit_response_model.fields_flask_type,
        )
        self._check_audit(self.TestController, [])


class MongoCRUDControllerBackupTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    class TestSecondController(database.CRUDController):
        pass

    _db = None

    @classmethod
    def setUpClass(cls):
        cls._db = database.load("mongomock", cls._create_models)
        cls.TestController.namespace(TestAPI)
        cls.TestSecondController.namespace(TestAPI)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info("Declare model class...")

        class TestModel(
            database_mongo.CRUDModel, base=base, table_name="sample_table_name"
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            mandatory = database_mongo.Column(int, is_nullable=False)
            optional = database_mongo.Column(str)

        class TestModelSecond(
            database_mongo.CRUDModel, base=base, table_name="second_sample_table_name"
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            mandatory = database_mongo.Column(int, is_nullable=False)
            optional = database_mongo.Column(str)

        logger.info("Save model class...")
        cls.TestController.model(TestModel)
        cls.TestSecondController.model(TestModelSecond)
        return [TestModel, TestModelSecond]

    def setUp(self):
        logger.info(f"-------------------------------")
        logger.info(f"Start of {self._testMethodName}")
        database.reset(self._db)

    def tearDown(self):
        logger.info(f"End of {self._testMethodName}")
        logger.info(f"-------------------------------")

    def test_dump_delete_restore_is_restoring_db_dumped(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.TestController.post(
            {"key": "my_key3", "mandatory": 3, "optional": "my_value3"}
        )
        self.TestSecondController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestSecondController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )

        with tempfile.TemporaryDirectory() as temp_directory:
            database.dump(self._db, temp_directory)
            self.TestController.delete({"key": "my_key1"})
            self.TestSecondController.delete({"key": "my_key1"})
            database.restore(self._db, temp_directory)

        self.assertEqual(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
                {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
            ],
            self.TestController.get({}),
        )
        self.assertEqual(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ],
            self.TestSecondController.get({}),
        )

    def test_dump_delete_all_restore_is_restoring_db_dumped(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.TestController.post(
            {"key": "my_key3", "mandatory": 3, "optional": "my_value3"}
        )
        self.TestSecondController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestSecondController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )

        with tempfile.TemporaryDirectory() as temp_directory:
            database.dump(self._db, temp_directory)
            self.TestController.delete({})
            self.TestSecondController.delete({})
            database.restore(self._db, temp_directory)

        self.assertEqual(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
                {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
            ],
            self.TestController.get({}),
        )
        self.assertEqual(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ],
            self.TestSecondController.get({}),
        )


class MongoCRUD2Entities1CollectionTest(unittest.TestCase):

    def setUp(self):
        self._db = database.load("mongomock", lambda x: x)

    def tearDown(self):
        database.reset(self._db)

    def test_2entities_on_same_collection_without_pk(self):
        class TestEntitySameCollection1(
            versioning_mongo.VersionedCRUDModel, base=self._db, table_name="sample_table_name_2entities"
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            mandatory = database_mongo.Column(int, is_nullable=False)
            optional = database_mongo.Column(str)

        TestEntitySameCollection1.add({'key': '1', 'mandatory': 2})
        TestEntitySameCollection1.add({'key': '2', 'mandatory': 2})

        with self.assertRaises(pymongo.errors.DuplicateKeyError):
            class TestEntitySameCollection2(
                versioning_mongo.VersionedCRUDModel, base=self._db, table_name="sample_table_name_2entities"
            ):
                pass

    def test_2entities_on_same_collection_with_pk(self):
        class TestEntitySameCollection1(
            versioning_mongo.VersionedCRUDModel, base=self._db, table_name="sample_table_name_2entities"
        ):
            key = database_mongo.Column(str, is_primary_key=True)
            mandatory = database_mongo.Column(int, is_nullable=False)
            optional = database_mongo.Column(str)

        TestEntitySameCollection1.add({'key': '1', 'mandatory': 2})
        TestEntitySameCollection1.add({'key': '2', 'mandatory': 2})

        class TestEntitySameCollection2(
            versioning_mongo.VersionedCRUDModel, base=self._db, table_name="sample_table_name_2entities", skip_update_indexes=True
        ):
            pass

        TestEntitySameCollection1.add({'key': '3', 'mandatory': 2})
        self.assertEqual(len(TestEntitySameCollection1.get_all()), 3)


if __name__ == "__main__":
    unittest.main()
