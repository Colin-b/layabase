import datetime
import logging
import sys
import unittest
from threading import Thread

import sqlalchemy
from flask_restplus import fields as flask_rest_plus_fields, inputs
from marshmallow_sqlalchemy.fields import fields as marshmallow_fields
from pycommon_error.validation import ValidationFailed, ModelCouldNotBeFound
from pycommon_test.flask_restplus_mock import TestAPI
from pycommon_test import mock_now, revert_now

logging.basicConfig(
    format="%(asctime)s [%(threadName)s] [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.DEBUG,
)
logging.getLogger("sqlalchemy").setLevel(logging.DEBUG)

from pycommon_database import database, database_sqlalchemy

logger = logging.getLogger(__name__)


def parser_types(flask_parser) -> dict:
    return {arg.name: arg.type for arg in flask_parser.args}


def parser_actions(flask_parser) -> dict:
    return {arg.name: arg.action for arg in flask_parser.args}


class SQlAlchemyDatabaseTest(unittest.TestCase):
    def setUp(self):
        logger.info(f"-------------------------------")
        logger.info(f"Start of {self._testMethodName}")
        self.maxDiff = None

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
            database.load("sqlite:///:memory:", None)
        self.assertEqual(
            "A method allowing to create related models must be provided.",
            str(cm.exception),
        )

    def test_models_are_added_to_metadata(self):
        def create_models(base):
            class TestModel(base):
                __tablename__ = "sample_table_name"

                key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

                @classmethod
                def _post_init(cls, base):
                    pass

            return [TestModel]

        db = database.load("sqlite:///:memory:", create_models)
        self.assertEqual("sqlite:///:memory:", str(db.metadata.bind.engine.url))
        self.assertEqual(
            ["sample_table_name"], [table for table in db.metadata.tables.keys()]
        )

    def test_sybase_url(self):
        self.assertEqual(
            "sybase+pyodbc:///?odbc_connect=TEST%3DVALUE%3BTEST2%3DVALUE2",
            database_sqlalchemy._clean_database_url(
                "sybase+pyodbc:///?odbc_connect=TEST=VALUE;TEST2=VALUE2"
            ),
        )

    def test_sybase_does_not_support_offset(self):
        self.assertFalse(database_sqlalchemy._supports_offset("sybase+pyodbc"))

    def test_sybase_does_not_support_retrieving_metadata(self):
        self.assertFalse(database_sqlalchemy._can_retrieve_metadata("sybase+pyodbc"))

    def test_mssql_url(self):
        self.assertEqual(
            "mssql+pyodbc:///?odbc_connect=TEST%3DVALUE%3BTEST2%3DVALUE2",
            database_sqlalchemy._clean_database_url(
                "mssql+pyodbc:///?odbc_connect=TEST=VALUE;TEST2=VALUE2"
            ),
        )

    def test_mssql_does_not_support_offset(self):
        self.assertFalse(database_sqlalchemy._supports_offset("mssql+pyodbc"))

    def test_mssql_does_not_support_retrieving_metadata(self):
        self.assertFalse(database_sqlalchemy._can_retrieve_metadata("mssql+pyodbc"))

    def test_sql_lite_support_offset(self):
        self.assertTrue(database_sqlalchemy._supports_offset("sqlite"))

    def test_in_memory_database_is_considered_as_in_memory(self):
        self.assertTrue(database_sqlalchemy._in_memory("sqlite:///:memory:"))

    def test_real_database_is_not_considered_as_in_memory(self):
        self.assertFalse(
            database_sqlalchemy._in_memory(
                "sybase+pyodbc:///?odbc_connect=TEST%3DVALUE%3BTEST2%3DVALUE2"
            )
        )


class SQlAlchemyCRUDModelTest(unittest.TestCase):
    _db = None
    _model = None

    @classmethod
    def setUpClass(cls):
        mock_now()
        cls._db = database.load("sqlite:///:memory:", cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info("Declare model class...")

        class TestModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = "sample_table_name"

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        class TestModelAutoIncr(database_sqlalchemy.CRUDModel, base):
            __tablename__ = "autoincre_table_name"

            key = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.String, nullable=False)

        logger.info("Save model class...")
        cls._model = TestModel
        cls._model_autoincr = TestModelAutoIncr
        return [TestModel, TestModelAutoIncr]

    def setUp(self):
        logger.info(f"-------------------------------")
        logger.info(f"Start of {self._testMethodName}")
        database.reset(self._db)

    def tearDown(self):
        logger.info(f"End of {self._testMethodName}")
        logger.info(f"-------------------------------")

    def test_health_details(self):
        health_status = database.health_details(self._db)
        expected_result = (
            "pass",
            {
                "sqlite:select": {
                    "componentType": "datastore",
                    "observedValue": "",
                    "status": "pass",
                    "time": "2018-10-11T15:05:05.663979",
                }
            },
        )
        self.assertEqual(expected_result, health_status)

    def test_health_details_no_db(self):
        with self.assertRaises(Exception) as cm:
            database.health_details(None)
        self.assertEqual(
            "A database connection URL must be provided.", str(cm.exception)
        )

    def test_get_all_without_data_returns_empty_list(self):
        self.assertEqual([], self._model.get_all())

    def test_get_without_data_returns_empty_dict(self):
        self.assertEqual({}, self._model.get())

    def test_add_with_nothing_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.add(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_primary_keys_are_returned(self):
        self.assertEqual(["key", "mandatory"], self._model.get_primary_keys())

    def test_add_with_empty_dict_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.add({})
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_update_with_nothing_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.update(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_update_with_empty_dict_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.update({})
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self.assertEqual({}, self._model.get())

    def test_remove_without_nothing_do_not_fail(self):
        self.assertEqual(0, self._model.remove())
        self.assertEqual({}, self._model.get())

    def test_add_without_mandatory_field_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.add({"key": "my_key"})
        self.assertEqual(
            {"mandatory": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual({"key": "my_key"}, cm.exception.received_data)
        self.assertEqual({}, self._model.get())

    def test_add_without_key_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.add({"mandatory": 1})
        self.assertEqual(
            {"key": ["Missing data for required field."]}, cm.exception.errors
        )
        self.assertEqual({"mandatory": 1}, cm.exception.received_data)
        self.assertEqual({}, self._model.get())

    def test_add_with_wrong_type_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.add({"key": 256, "mandatory": 1})
        self.assertEqual({"key": ["Not a valid string."]}, cm.exception.errors)
        self.assertEqual({"key": 256, "mandatory": 1}, cm.exception.received_data)
        self.assertEqual({}, self._model.get())

    def test_update_with_wrong_type_is_invalid(self):
        self._model.add({"key": "value1", "mandatory": 1})
        with self.assertRaises(ValidationFailed) as cm:
            self._model.update({"key": "value1", "mandatory": "invalid_value"})
        self.assertEqual({"mandatory": ["Not a valid integer."]}, cm.exception.errors)
        self.assertEqual(
            {"key": "value1", "mandatory": "invalid_value"}, cm.exception.received_data
        )

    def test_add_all_with_nothing_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.add_all(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)

    def test_add_all_with_empty_dict_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.add_all({})
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_add_all_without_mandatory_field_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.add_all(
                [
                    {"key": "my_key"},
                    {"key": "my_key", "mandatory": 1, "optional": "my_value"},
                ]
            )
        self.assertEqual(
            {0: {"mandatory": ["Missing data for required field."]}},
            cm.exception.errors,
        )
        self.assertEqual(
            [
                {"key": "my_key"},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ],
            cm.exception.received_data,
        )
        self.assertEqual({}, self._model.get())

    def test_add_all_without_key_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.add_all(
                [
                    {"mandatory": 1},
                    {"key": "my_key", "mandatory": 1, "optional": "my_value"},
                ]
            )
        self.assertEqual(
            {0: {"key": ["Missing data for required field."]}}, cm.exception.errors
        )
        self.assertEqual(
            [
                {"mandatory": 1},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ],
            cm.exception.received_data,
        )
        self.assertEqual({}, self._model.get())

    def test_add_all_with_wrong_type_is_invalid(self):
        with self.assertRaises(ValidationFailed) as cm:
            self._model.add_all(
                [
                    {"key": 256, "mandatory": 1},
                    {"key": "my_key", "mandatory": 1, "optional": "my_value"},
                ]
            )
        self.assertEqual({0: {"key": ["Not a valid string."]}}, cm.exception.errors)
        self.assertEqual(
            [
                {"key": 256, "mandatory": 1},
                {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            ],
            cm.exception.received_data,
        )
        self.assertEqual({}, self._model.get())

    def test_add_without_optional_is_valid(self):
        self.assertEqual(
            {"mandatory": 1, "key": "my_key", "optional": None},
            self._model.add({"key": "my_key", "mandatory": 1}),
        )
        self.assertEqual(
            {"key": "my_key", "mandatory": 1, "optional": None}, self._model.get()
        )

    def test_add_with_optional_is_valid(self):
        self.assertEqual(
            {"mandatory": 1, "key": "my_key", "optional": "my_value"},
            self._model.add({"key": "my_key", "mandatory": 1, "optional": "my_value"}),
        )
        self.assertEqual(
            {"key": "my_key", "mandatory": 1, "optional": "my_value"}, self._model.get()
        )

    def test_update_unexisting_is_invalid(self):
        with self.assertRaises(ModelCouldNotBeFound) as cm:
            self._model.update(
                {"key": "my_key", "mandatory": 1, "optional": "my_value"}
            )
        self.assertEqual(
            {"key": "my_key", "mandatory": 1, "optional": "my_value"},
            cm.exception.requested_data,
        )

    def test_add_with_unknown_field_is_valid(self):
        self.assertEqual(
            {"mandatory": 1, "key": "my_key", "optional": "my_value"},
            self._model.add(
                {
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": "my_value",
                    # This field do not exists in schema
                    "unknown": "my_value",
                }
            ),
        )
        self.assertEqual(
            {"key": "my_key", "mandatory": 1, "optional": "my_value"}, self._model.get()
        )

    def test_get_without_filter_is_retrieving_the_only_item(self):
        self._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
        self.assertEqual(
            {"mandatory": 1, "optional": "my_value1", "key": "my_key1"},
            self._model.get(),
        )

    def test_get_without_filter_is_failing_if_more_than_one_item_exists(self):
        self._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
        self._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
        with self.assertRaises(ValidationFailed) as cm:
            self._model.get()
        self.assertEqual(
            {"": ["More than one result: Consider another filtering."]},
            cm.exception.errors,
        )
        self.assertEqual({}, cm.exception.received_data)

    def test_get_all_without_filter_is_retrieving_everything_after_multiple_posts(self):
        self._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
        self._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
        self.assertEqual(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ],
            self._model.get_all(),
        )

    def test_get_all_without_filter_is_retrieving_everything(self):
        self._model.add_all(
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
            self._model.get_all(),
        )

    def test_get_all_with_filter_is_retrieving_subset_after_multiple_posts(self):
        self._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
        self._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
        self.assertEqual(
            [{"key": "my_key1", "mandatory": 1, "optional": "my_value1"}],
            self._model.get_all(optional="my_value1"),
        )

    def test_get_all_with_filter_is_retrieving_subset(self):
        self._model.add_all(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ]
        )
        self.assertEqual(
            [{"key": "my_key1", "mandatory": 1, "optional": "my_value1"}],
            self._model.get_all(optional="my_value1"),
        )

    def test_get_all_order_by(self):
        self._model.add_all(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 1, "optional": "my_value2"},
                {"key": "my_key3", "mandatory": -1, "optional": "my_value3"},
            ]
        )
        self.assertEqual(
            [
                {"key": "my_key3", "mandatory": -1, "optional": "my_value3"},
                {"key": "my_key2", "mandatory": 1, "optional": "my_value2"},
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            ],
            self._model.get_all(
                order_by=[
                    sqlalchemy.asc(self._model.mandatory),
                    sqlalchemy.desc(self._model.key),
                ]
            ),
        )

    def test_get_with_filter_is_retrieving_the_proper_row_after_multiple_posts(self):
        self._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
        self._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
        self.assertEqual(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            self._model.get(optional="my_value1"),
        )

    def test_get_with_filter_is_retrieving_the_proper_row(self):
        self._model.add_all(
            [
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
            ]
        )
        self.assertEqual(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            self._model.get(optional="my_value1"),
        )

    def test_update_is_updating(self):
        self._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
        self.assertEqual(
            (
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
                {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
            ),
            self._model.update({"key": "my_key1", "optional": "my_value"}),
        )
        self.assertEqual(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value"},
            self._model.get(mandatory=1),
        )

    def test_update_is_updating_and_previous_value_cannot_be_used_to_filter(self):
        self._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
        self._model.update({"key": "my_key1", "optional": "my_value"})
        self.assertEqual({}, self._model.get(optional="my_value1"))

    def test_remove_with_filter_is_removing_the_proper_row(self):
        self._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
        self._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
        self.assertEqual(1, self._model.remove(key="my_key1"))
        self.assertEqual(
            [{"key": "my_key2", "mandatory": 2, "optional": "my_value2"}],
            self._model.get_all(),
        )

    def test_remove_without_filter_is_removing_everything(self):
        self._model.add({"key": "my_key1", "mandatory": 1, "optional": "my_value1"})
        self._model.add({"key": "my_key2", "mandatory": 2, "optional": "my_value2"})
        self.assertEqual(2, self._model.remove())
        self.assertEqual([], self._model.get_all())


class SQLAlchemyCRUDControllerTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    class TestAutoIncrementController(database.CRUDController):
        pass

    class TestDateController(database.CRUDController):
        pass

    class TestInheritanceController(database.CRUDController):
        pass

    class TestLikeOperatorController(database.CRUDController):
        pass

    _db = None

    @classmethod
    def setUpClass(cls):
        revert_now()
        cls._db = database.load("sqlite:///:memory:", cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info("Declare model class...")

        class TestModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = "sample_table_name"

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        class TestAutoIncrementModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = "auto_increment_table_name"

            key = sqlalchemy.Column(
                sqlalchemy.Integer, primary_key=True, autoincrement=True
            )
            enum_field = sqlalchemy.Column(
                sqlalchemy.Enum("Value1", "Value2"),
                nullable=False,
                doc="Test Documentation",
            )
            optional_with_default = sqlalchemy.Column(
                sqlalchemy.String, default="Test value"
            )

        class TestDateModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = "date_table_name"

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            date_str = sqlalchemy.Column(sqlalchemy.Date)
            datetime_str = sqlalchemy.Column(sqlalchemy.DateTime)

        class Inherited:
            optional = sqlalchemy.Column(sqlalchemy.String)

        class TestInheritanceModel(database_sqlalchemy.CRUDModel, Inherited, base):
            __tablename__ = "inheritance_table_name"

            key = sqlalchemy.Column(
                sqlalchemy.Integer, primary_key=True, autoincrement=True
            )
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)

        TestInheritanceModel.audit()

        class TestLikeOperatorModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = "like_operator_table_name"

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

        TestLikeOperatorModel.interpret_star_character_as_like()

        logger.info("Save model class...")
        cls.TestController.model(TestModel)
        cls.TestController.namespace(TestAPI)
        cls.TestAutoIncrementController.model(TestAutoIncrementModel)
        cls.TestAutoIncrementController.namespace(TestAPI)
        cls.TestDateController.model(TestDateModel)
        cls.TestDateController.namespace(TestAPI)
        cls.TestInheritanceController.model(TestInheritanceModel)
        cls.TestInheritanceController.namespace(TestAPI)
        cls.TestLikeOperatorController.model(TestLikeOperatorModel)
        cls.TestLikeOperatorController.namespace(TestAPI)
        return [
            TestModel,
            TestAutoIncrementModel,
            TestDateModel,
            TestInheritanceModel,
            TestLikeOperatorModel,
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

    def test_post_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_post_list_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_post_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({})
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_post_many_with_empty_list_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([])
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_put_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.put(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_put_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.put({})
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
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
            self.TestController.post({"key": 256, "mandatory": 1})
        self.assertEqual({"key": ["Not a valid string."]}, cm.exception.errors)
        self.assertEqual({"key": 256, "mandatory": 1}, cm.exception.received_data)

    def test_post_many_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{"key": 256, "mandatory": 1}])
        self.assertEqual({0: {"key": ["Not a valid string."]}}, cm.exception.errors)
        self.assertEqual([{"key": 256, "mandatory": 1}], cm.exception.received_data)

    def test_put_with_wrong_type_is_invalid(self):
        self.TestController.post({"key": "value1", "mandatory": 1})
        with self.assertRaises(Exception) as cm:
            self.TestController.put({"key": "value1", "mandatory": "invalid value"})
        self.assertEqual({"mandatory": ["Not a valid integer."]}, cm.exception.errors)
        self.assertEqual(
            {"key": "value1", "mandatory": "invalid value"}, cm.exception.received_data
        )

    def test_post_without_optional_is_valid(self):
        self.assertEqual(
            {"mandatory": 1, "key": "my_key", "optional": None},
            self.TestController.post({"key": "my_key", "mandatory": 1}),
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

    def test_get_like_operator_double_star(self):
        self.TestLikeOperatorController.post_many(
            [{"key": "my_key"}, {"key": "my_key2"}, {"key": "my_ey"}]
        )
        self.assertEqual(
            [{"key": "my_key"}, {"key": "my_key2"}],
            self.TestLikeOperatorController.get({"key": "*y_k*"}),
        )

    def test_get_like_operator_star_at_start(self):
        self.TestLikeOperatorController.post_many(
            [{"key": "my_key"}, {"key": "my_key2"}, {"key": "my_ey"}, {"key": "my_k"}]
        )
        self.assertEqual(
            [{"key": "my_k"}], self.TestLikeOperatorController.get({"key": "*y_k"})
        )

    def test_get_like_operator_star_at_end(self):
        self.TestLikeOperatorController.post_many(
            [
                {"key": "my_key"},
                {"key": "my_key2"},
                {"key": "my_ey"},
                {"key": "my_k"},
                {"key": "y_key"},
            ]
        )
        self.assertEqual(
            [{"key": "y_key"}], self.TestLikeOperatorController.get({"key": "y_k*"})
        )

    def test_get_like_operator_no_star(self):
        self.TestLikeOperatorController.post_many(
            [
                {"key": "my_key"},
                {"key": "my_key2"},
                {"key": "my_ey"},
                {"key": "my_k"},
                {"key": "y_key"},
            ]
        )
        self.assertEqual(
            [{"key": "my_key"}], self.TestLikeOperatorController.get({"key": "my_key"})
        )

    def test_get_like_operator_no_star_no_result(self):
        self.TestLikeOperatorController.post_many(
            [
                {"key": "my_key"},
                {"key": "my_key2"},
                {"key": "my_ey"},
                {"key": "my_k"},
                {"key": "y_key"},
            ]
        )
        self.assertEqual([], self.TestLikeOperatorController.get({"key": "y_k"}))

    def test_get_no_like_operator(self):
        self.TestController.post_many(
            [
                {"key": "my_key", "mandatory": 1},
                {"key": "my_key2", "mandatory": 1},
                {"key": "my_ey", "mandatory": 1},
                {"key": "my_k", "mandatory": 1},
                {"key": "y_key", "mandatory": 1},
            ]
        )
        self.assertEqual([], self.TestController.get({"key": "*y_k*"}))

    def test_post_with_optional_is_valid(self):
        self.assertEqual(
            {"mandatory": 1, "key": "my_key", "optional": "my_value"},
            self.TestController.post(
                {"key": "my_key", "mandatory": 1, "optional": "my_value"}
            ),
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

    def test_get_with_list_filter_matching_one_is_retrieving_subset(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(
            [{"key": "my_key1", "mandatory": 1, "optional": "my_value1"}],
            self.TestController.get({"optional": ["my_value1"]}),
        )

    def test_get_with_list_filter_matching_many_is_retrieving_subset(self):
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
            self.TestController.get({"optional": ["my_value1", "my_value2"]}),
        )

    def test_get_with_list_filter_matching_partial_is_retrieving_subset(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(
            [{"key": "my_key1", "mandatory": 1, "optional": "my_value1"}],
            self.TestController.get(
                {"optional": ["non existing", "my_value1", "not existing"]}
            ),
        )

    def test_get_with_empty_list_filter_is_retrieving_everything(self):
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
            self.TestController.get({"optional": []}),
        )

    def test_delete_with_list_filter_matching_one_is_retrieving_subset(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(1, self.TestController.delete({"optional": ["my_value1"]}))

    def test_delete_with_list_filter_matching_many_is_retrieving_subset(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(
            2, self.TestController.delete({"optional": ["my_value1", "my_value2"]})
        )

    def test_delete_with_list_filter_matching_partial_is_retrieving_subset(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(
            1,
            self.TestController.delete(
                {"optional": ["non existing", "my_value1", "not existing"]}
            ),
        )

    def test_delete_with_empty_list_filter_is_retrieving_everything(self):
        self.TestController.post(
            {"key": "my_key1", "mandatory": 1, "optional": "my_value1"}
        )
        self.TestController.post(
            {"key": "my_key2", "mandatory": 2, "optional": "my_value2"}
        )
        self.assertEqual(2, self.TestController.delete({"optional": []}))

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
                    "datetime_str": "2016-09-23T23:59:59+00:00",
                    "key": "my_key1",
                },
                {
                    "date_str": "2018-06-01",
                    "datetime_str": "1989-12-31T01:00:00+00:00",
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
                    "datetime_str": "1989-12-31T01:00:00+00:00",
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
                    "datetime_str": "2016-09-23T23:59:59+00:00",
                    "key": "my_key1",
                }
            ],
            self.TestDateController.get({"date_str": d}),
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
                    "datetime_str": "2016-09-23T23:59:59+00:00",
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
                "order_by": str,
                "offset": inputs.natural,
            },
            parser_types(self.TestController.query_get_parser),
        )

    def test_query_delete_parser(self):
        self.assertEqual(
            {"key": str, "mandatory": int, "optional": str},
            parser_types(self.TestController.query_delete_parser),
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

    def test_get_with_order_by_desc_is_retrieving_elements_ordered_by_descending_mode(
        self
    ):
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
                {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
                {"key": "my_key2", "mandatory": 2, "optional": "my_value2"},
                {"key": "my_key1", "mandatory": 1, "optional": "my_value1"},
            ],
            self.TestController.get({"order_by": ["key desc"]}),
        )

    def test_get_with_order_by_is_retrieving_elements_ordered_by_ascending_mode(self):
        self.TestController.post(
            {"key": "my_key3", "mandatory": 3, "optional": "my_value3"}
        )
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
                {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
            ],
            self.TestController.get({"order_by": ["key"]}),
        )

    def test_get_with_2_order_by_is_retrieving_elements_ordered_by(self):
        self.TestController.post(
            {"key": "my_key3", "mandatory": 3, "optional": "my_value3"}
        )
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
                {"key": "my_key3", "mandatory": 3, "optional": "my_value3"},
            ],
            self.TestController.get({"order_by": ["key", "mandatory desc"]}),
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
                "table": "sample_table_name",
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
                "key": "String",
                "mandatory": "String",
                "optional": "String",
                "table": "String",
            },
            self.TestController.get_model_description_response_model.fields_flask_type,
        )


class SQLAlchemyCRUDControllerFailuresTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    _db = None

    @classmethod
    def setUpClass(cls):
        cls._db = database.load("sqlite:///:memory:", cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info("Declare model class...")

        class TestModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = "sample_table_name"

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

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


class SQLAlchemyCRUDControllerAuditTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    class Test2Controller(database.CRUDController):
        pass

    _db = None

    @classmethod
    def setUpClass(cls):
        revert_now()
        cls._db = database.load("sqlite:///:memory:", cls._create_models)
        cls.TestController.namespace(TestAPI)
        cls.Test2Controller.namespace(TestAPI)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        class TestModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = "sample_table_name"

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        TestModel.audit()

        class Test2Model(database_sqlalchemy.CRUDModel, base):
            __tablename__ = "sample2_table_name"

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        Test2Model.audit()

        cls.TestController.model(TestModel)
        cls.Test2Controller.model(Test2Model)
        return [TestModel, Test2Model]

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

    def test_get_parser_fields_order(self):
        self.assertEqual(
            ["key", "mandatory", "optional", "limit", "order_by", "offset"],
            [arg.name for arg in self.TestController.query_get_parser.args],
        )

    def test_delete_parser_fields_order(self):
        self.assertEqual(
            ["key", "mandatory", "optional"],
            [arg.name for arg in self.TestController.query_delete_parser.args],
        )

    def test_post_model_fields_order(self):
        self.assertEqual(
            {"key": "String", "mandatory": "Integer", "optional": "String"},
            self.TestController.json_post_model.fields_flask_type,
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
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_post_many_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_post_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({})
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_post_many_with_empty_list_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([])
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_put_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.put(None)
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_put_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.put({})
        self.assertEqual({"": ["No data provided."]}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_delete_without_nothing_do_not_fail(self):
        self.assertEqual(0, self.TestController.delete({}))
        self._check_audit(self.TestController, [])

    def test_post_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
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
            self.TestController.post({"key": 256, "mandatory": 1})
        self.assertEqual({"key": ["Not a valid string."]}, cm.exception.errors)
        self.assertEqual({"key": 256, "mandatory": 1}, cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_post_many_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{"key": 256, "mandatory": 1}])
        self.assertEqual({0: {"key": ["Not a valid string."]}}, cm.exception.errors)
        self.assertEqual([{"key": 256, "mandatory": 1}], cm.exception.received_data)
        self._check_audit(self.TestController, [])

    def test_put_with_wrong_type_is_invalid(self):
        self.TestController.post({"key": "value1", "mandatory": 1})
        with self.assertRaises(Exception) as cm:
            self.TestController.put({"key": "value1", "mandatory": "invalid_value"})
        self.assertEqual({"mandatory": ["Not a valid integer."]}, cm.exception.errors)
        self.assertEqual(
            {"key": "value1", "mandatory": "invalid_value"}, cm.exception.received_data
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "value1",
                    "mandatory": 1,
                    "optional": None,
                    "revision": 1,
                }
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": None,
                    "revision": 1,
                }
            ],
        )

    def test_post_on_a_second_model_without_optional_is_valid(self):
        self.TestController.post({"key": "my_key", "mandatory": 1})
        self.assertEqual(
            {"optional": None, "mandatory": 1, "key": "my_key"},
            self.Test2Controller.post({"key": "my_key", "mandatory": 1}),
        )
        self._check_audit(
            self.Test2Controller,
            [
                {
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": None,
                    "revision": 1,
                }
            ],
        )
        self._check_audit(
            self.TestController,
            [
                {
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": None,
                    "revision": 1,
                }
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key",
                    "mandatory": 1,
                    "optional": None,
                    "revision": 1,
                }
            ],
        )

    def _check_audit(self, controller, expected_audit, filter_audit=None):
        if not filter_audit:
            filter_audit = {}
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "U",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "U",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key2",
                    "mandatory": 2,
                    "optional": "my_value2",
                    "revision": 2,
                },
                {
                    "audit_action": "D",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "U",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 2,
                    "optional": "my_value1",
                    "revision": 2,
                },
                {
                    "audit_action": "D",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                    "audit_action": "U",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 2,
                    "optional": "my_value1",
                    "revision": 2,
                }
            ],
            filter_audit={"audit_action": "U"},
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
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 1,
                },
                {
                    "audit_action": "I",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key2",
                    "mandatory": 2,
                    "optional": "my_value2",
                    "revision": 2,
                },
                {
                    "audit_action": "D",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
                    "audit_user": "",
                    "key": "my_key1",
                    "mandatory": 1,
                    "optional": "my_value1",
                    "revision": 3,
                },
                {
                    "audit_action": "D",
                    "audit_date_utc": "\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d\d\d\d\d\d)?\+00:00",
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
                "order_by": str,
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
                "order_by": str,
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


class SQLAlchemyFlaskRestPlusModelsTest(unittest.TestCase):
    def setUp(self):
        logger.info(f"-------------------------------")
        logger.info(f"Start of {self._testMethodName}")

    def tearDown(self):
        logger.info(f"End of {self._testMethodName}")
        logger.info(f"-------------------------------")

    def test_rest_plus_type_for_string_field_is_string(self):
        field = marshmallow_fields.String()
        self.assertEqual(
            flask_rest_plus_fields.String,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_int_field_is_integer(self):
        field = marshmallow_fields.Integer()
        self.assertEqual(
            flask_rest_plus_fields.Integer,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_bool_field_is_boolean(self):
        field = marshmallow_fields.Boolean()
        self.assertEqual(
            flask_rest_plus_fields.Boolean,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_date_field_is_date(self):
        field = marshmallow_fields.Date()
        self.assertEqual(
            flask_rest_plus_fields.Date, database_sqlalchemy._get_rest_plus_type(field)
        )

    def test_rest_plus_type_for_datetime_field_is_datetime(self):
        field = marshmallow_fields.DateTime()
        self.assertEqual(
            flask_rest_plus_fields.DateTime,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_decimal_field_is_fixed(self):
        field = marshmallow_fields.Decimal()
        self.assertEqual(
            flask_rest_plus_fields.Fixed, database_sqlalchemy._get_rest_plus_type(field)
        )

    def test_rest_plus_type_for_float_field_is_float(self):
        field = marshmallow_fields.Float()
        self.assertEqual(
            flask_rest_plus_fields.Float, database_sqlalchemy._get_rest_plus_type(field)
        )

    def test_rest_plus_type_for_number_field_is_decimal(self):
        field = marshmallow_fields.Number()
        self.assertEqual(
            flask_rest_plus_fields.Decimal,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_time_field_is_datetime(self):
        field = marshmallow_fields.Time()
        self.assertEqual(
            flask_rest_plus_fields.DateTime,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_field_field_is_string(self):
        field = marshmallow_fields.Field()
        self.assertEqual(
            flask_rest_plus_fields.String,
            database_sqlalchemy._get_rest_plus_type(field),
        )

    def test_rest_plus_type_for_none_field_cannot_be_guessed(self):
        with self.assertRaises(Exception) as cm:
            database_sqlalchemy._get_rest_plus_type(None)
        self.assertEqual(
            "Flask RestPlus field type cannot be guessed for None field.",
            cm.exception.args[0],
        )

    def test_rest_plus_example_for_string_field(self):
        field = marshmallow_fields.String()
        self.assertEqual("sample_value", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_int_field_is_integer(self):
        field = marshmallow_fields.Integer()
        self.assertEqual("0", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_bool_field_is_true(self):
        field = marshmallow_fields.Boolean()
        self.assertEqual("true", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_date_field_is_YYYY_MM_DD(self):
        field = marshmallow_fields.Date()
        self.assertEqual("2017-09-24", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_datetime_field_is_YYYY_MM_DDTHH_MM_SS(self):
        field = marshmallow_fields.DateTime()
        self.assertEqual("2017-09-24T15:36:09", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_decimal_field_is_decimal(self):
        field = marshmallow_fields.Decimal()
        self.assertEqual("0.0", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_float_field_is_float(self):
        field = marshmallow_fields.Float()
        self.assertEqual("0.0", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_number_field_is_decimal(self):
        field = marshmallow_fields.Number()
        self.assertEqual("0.0", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_time_field_is_HH_MM_SS(self):
        field = marshmallow_fields.Time()
        self.assertEqual("15:36:09", database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_none_field_is_sample_value(self):
        self.assertEqual("sample_value", database_sqlalchemy._get_example(None))


class SQlAlchemyColumnsTest(unittest.TestCase):
    _model = None

    @classmethod
    def setUpClass(cls):
        revert_now()
        database.load("sqlite:///:memory:", cls._create_models)

    @classmethod
    def _create_models(cls, base):
        logger.info("Declare model class...")

        class TestModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = "sample_table_name"

            string_column = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            integer_column = sqlalchemy.Column(sqlalchemy.Integer)
            boolean_column = sqlalchemy.Column(sqlalchemy.Boolean)
            date_column = sqlalchemy.Column(sqlalchemy.Date)
            datetime_column = sqlalchemy.Column(sqlalchemy.DateTime)
            float_column = sqlalchemy.Column(sqlalchemy.Float)

        logger.info("Save model class...")
        cls._model = TestModel
        return [TestModel]

    def setUp(self):
        logger.info(f"-------------------------------")
        logger.info(f"Start of {self._testMethodName}")

    def tearDown(self):
        logger.info(f"End of {self._testMethodName}")
        logger.info(f"-------------------------------")

    def test_field_declaration_order_is_kept_in_schema(self):
        fields = self._model.schema().fields
        self.assertEqual(
            [
                "string_column",
                "integer_column",
                "boolean_column",
                "date_column",
                "datetime_column",
                "float_column",
            ],
            [field_name for field_name in fields],
        )

    def test_python_type_for_sqlalchemy_string_field_is_string(self):
        field = self._model.schema().fields["string_column"]
        self.assertEqual(str, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_integer_field_is_integer(self):
        field = self._model.schema().fields["integer_column"]
        self.assertEqual(int, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_boolean_field_is_boolean(self):
        field = self._model.schema().fields["boolean_column"]
        self.assertEqual(inputs.boolean, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_date_field_is_date(self):
        field = self._model.schema().fields["date_column"]
        self.assertEqual(
            inputs.date_from_iso8601, database_sqlalchemy._get_python_type(field)
        )

    def test_python_type_for_sqlalchemy_datetime_field_is_datetime(self):
        field = self._model.schema().fields["datetime_column"]
        self.assertEqual(
            inputs.datetime_from_iso8601, database_sqlalchemy._get_python_type(field)
        )

    def test_python_type_for_sqlalchemy_float_field_is_float(self):
        field = self._model.schema().fields["float_column"]
        self.assertEqual(float, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_none_field_cannot_be_guessed(self):
        with self.assertRaises(Exception) as cm:
            database_sqlalchemy._get_python_type(None)
        self.assertEqual(
            "Python field type cannot be guessed for None field.", cm.exception.args[0]
        )


if __name__ == "__main__":
    unittest.main()
