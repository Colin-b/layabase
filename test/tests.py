import unittest
import logging
import sqlalchemy
import sys
import datetime
from marshmallow_sqlalchemy.fields import fields as marshmallow_fields
from flask_restplus import fields as flask_rest_plus_fields, inputs
from threading import Thread
import time

logging.basicConfig(
    format='%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.DEBUG)
logging.getLogger('sqlalchemy').setLevel(logging.DEBUG)

from pycommon_database import database, flask_restplus_errors, database_sqlalchemy, database_mongo

logger = logging.getLogger(__name__)


class SQlAlchemyDatabaseTest(unittest.TestCase):
    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')
        self.maxDiff = None

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_none_connection_string_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            database.load(None, None)
        self.assertEqual('A database connection URL must be provided.', cm.exception.args[0])

    def test_empty_connection_string_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            database.load('', None)
        self.assertEqual('A database connection URL must be provided.', cm.exception.args[0])

    def test_no_create_models_function_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            database.load('sqlite:///:memory:', None)
        self.assertEqual('A method allowing to create related models must be provided.', cm.exception.args[0])

    def test_models_are_added_to_metadata(self):
        def create_models(base):
            class TestModel(base):
                __tablename__ = 'sample_table_name'

                key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

            return [TestModel]

        db = database.load('sqlite:///:memory:', create_models)
        self.assertEqual('sqlite:///:memory:', str(db.metadata.bind.engine.url))
        self.assertEqual(['sample_table_name'], [table for table in db.metadata.tables.keys()])

    def test_sybase_url(self):
        self.assertEqual('sybase+pyodbc:///?odbc_connect=TEST%3DVALUE%3BTEST2%3DVALUE2',
                         database_sqlalchemy._clean_database_url('sybase+pyodbc:///?odbc_connect=TEST=VALUE;TEST2=VALUE2'))

    def test_sybase_does_not_support_offset(self):
        self.assertFalse(database_sqlalchemy._supports_offset('sybase+pyodbc'))

    def test_sybase_does_not_support_retrieving_metadata(self):
        self.assertFalse(database_sqlalchemy._can_retrieve_metadata('sybase+pyodbc'))

    def test_mssql_url(self):
        self.assertEqual('mssql+pyodbc:///?odbc_connect=TEST%3DVALUE%3BTEST2%3DVALUE2',
                         database_sqlalchemy._clean_database_url('mssql+pyodbc:///?odbc_connect=TEST=VALUE;TEST2=VALUE2'))

    def test_mssql_does_not_support_offset(self):
        self.assertFalse(database_sqlalchemy._supports_offset('mssql+pyodbc'))

    def test_mssql_does_not_support_retrieving_metadata(self):
        self.assertFalse(database_sqlalchemy._can_retrieve_metadata('mssql+pyodbc'))

    def test_sql_lite_support_offset(self):
        self.assertTrue(database_sqlalchemy._supports_offset('sqlite'))

    def test_in_memory_database_is_considered_as_in_memory(self):
        self.assertTrue(database_sqlalchemy._in_memory('sqlite:///:memory:'))

    def test_real_database_is_not_considered_as_in_memory(self):
        self.assertFalse(database_sqlalchemy._in_memory('sybase+pyodbc:///?odbc_connect=TEST%3DVALUE%3BTEST2%3DVALUE2'))


class SQlAlchemyCRUDModelTest(unittest.TestCase):
    _db = None
    _model = None

    @classmethod
    def setUpClass(cls):
        cls._db = database.load('sqlite:///:memory:', cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        class TestModelAutoIncr(database_sqlalchemy.CRUDModel, base):
            __tablename__ = 'autoincre_table_name'

            key = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.String, nullable=False)

        logger.info('Save model class...')
        cls._model = TestModel
        cls._model_autoincr = TestModelAutoIncr
        return [TestModel, TestModelAutoIncr]

    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')
        database.reset(self._db)

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_get_all_without_data_returns_empty_list(self):
        self.assertEqual([], self._model.get_all())

    def test_get_without_data_returns_empty_dict(self):
        self.assertEqual({}, self._model.get())

    def test_add_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.add(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_add_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.add({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_update_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.update(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_update_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.update({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self.assertEqual({}, self._model.get())

    def test_remove_without_nothing_do_not_fail(self):
        self.assertEqual(0, self._model.remove())
        self.assertEqual({}, self._model.get())

    def test_add_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.add({
                'key': 'my_key',
            })
        self.assertEqual({'mandatory': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'key': 'my_key'}, cm.exception.received_data)
        self.assertEqual({}, self._model.get())

    def test_add_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.add({
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'mandatory': 1}, cm.exception.received_data)
        self.assertEqual({}, self._model.get())

    def test_add_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.add({
                'key': 256,
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Not a valid string.']}, cm.exception.errors)
        self.assertEqual({'key': 256, 'mandatory': 1}, cm.exception.received_data)
        self.assertEqual({}, self._model.get())

    def test_update_with_wrong_type_is_invalid(self):
        self._model.add({
            'key': 'value1',
            'mandatory': 1,
        })
        with self.assertRaises(Exception) as cm:
            self._model.update({
                'key': 'value1',
                'mandatory': 'invalid_value',
            })
        self.assertEqual({'mandatory': ['Not a valid integer.']}, cm.exception.errors)
        self.assertEqual({'key': 'value1', 'mandatory': 'invalid_value'}, cm.exception.received_data)

    def test_add_all_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.add_all(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)

    def test_add_all_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.add_all({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_add_all_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.add_all([{
                'key': 'my_key',
            },
                {
                    'key': 'my_key',
                    'mandatory': 1,
                    'optional': 'my_value',
                }
            ])
        self.assertEqual({0: {'mandatory': ['Missing data for required field.']}}, cm.exception.errors)
        self.assertEqual([{'key': 'my_key'}, {'key': 'my_key', 'mandatory': 1, 'optional': 'my_value'}],
                         cm.exception.received_data)
        self.assertEqual({}, self._model.get())

    def test_add_all_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.add_all([{
                'mandatory': 1,
            },
                {
                    'key': 'my_key',
                    'mandatory': 1,
                    'optional': 'my_value',
                }]
            )
        self.assertEqual({0: {'key': ['Missing data for required field.']}}, cm.exception.errors)
        self.assertEqual([{'mandatory': 1}, {'key': 'my_key', 'mandatory': 1, 'optional': 'my_value'}],
                         cm.exception.received_data)
        self.assertEqual({}, self._model.get())

    def test_add_all_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self._model.add_all([{
                'key': 256,
                'mandatory': 1,
            },
                {
                    'key': 'my_key',
                    'mandatory': 1,
                    'optional': 'my_value',
                }]
            )
        self.assertEqual({0: {'key': ['Not a valid string.']}}, cm.exception.errors)
        self.assertEqual([{'key': 256, 'mandatory': 1}, {'key': 'my_key', 'mandatory': 1, 'optional': 'my_value'}],
                         cm.exception.received_data)
        self.assertEqual({}, self._model.get())

    def test_add_without_optional_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': None},
            self._model.add({
                'key': 'my_key',
                'mandatory': 1,
            })
        )
        self.assertEqual({'key': 'my_key', 'mandatory': 1, 'optional': None}, self._model.get())

    def test_add_with_optional_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
            self._model.add({
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
            })
        )
        self.assertEqual({'key': 'my_key', 'mandatory': 1, 'optional': 'my_value'}, self._model.get())

    def test_add_with_unknown_field_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
            self._model.add({
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
                # This field do not exists in schema
                'unknown': 'my_value',
            })
        )
        self.assertEqual({'key': 'my_key', 'mandatory': 1, 'optional': 'my_value'}, self._model.get())

    def test_get_without_filter_is_retrieving_the_only_item(self):
        self._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.assertEqual(
            {
                'mandatory': 1,
                'optional': 'my_value1',
                'key': 'my_key1'
            },
            self._model.get())

    def test_get_without_filter_is_failing_if_more_than_one_item_exists(self):
        self._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        with self.assertRaises(Exception) as cm:
            self._model.get()
        self.assertEqual({'': ['More than one result: Consider another filtering.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_get_all_without_filter_is_retrieving_everything_after_multiple_posts(self):
        self._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}
            ],
            self._model.get_all())

    def test_get_all_without_filter_is_retrieving_everything(self):
        self._model.add_all([
            {
                'key': 'my_key1',
                'mandatory': 1,
                'optional': 'my_value1',
            },
            {
                'key': 'my_key2',
                'mandatory': 2,
                'optional': 'my_value2',
            }
        ])
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}
            ],
            self._model.get_all())

    def test_get_all_with_filter_is_retrieving_subset_after_multiple_posts(self):
        self._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
            ],
            self._model.get_all(optional='my_value1'))

    def test_get_all_with_filter_is_retrieving_subset(self):
        self._model.add_all([
            {
                'key': 'my_key1',
                'mandatory': 1,
                'optional': 'my_value1',
            },
            {
                'key': 'my_key2',
                'mandatory': 2,
                'optional': 'my_value2',
            }
        ])
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
            ],
            self._model.get_all(optional='my_value1'))

    def test_get_all_order_by(self):
        self._model.add_all([
            {
                'key': 'my_key1',
                'mandatory': 1,
                'optional': 'my_value1',
            },
            {
                'key': 'my_key2',
                'mandatory': 1,
                'optional': 'my_value2',
            },
            {'key': 'my_key3', 'mandatory': -1, 'optional': 'my_value3'},

        ])
        self.assertEqual(
            [
                {'key': 'my_key3', 'mandatory': -1, 'optional': 'my_value3'},
                {'key': 'my_key2', 'mandatory': 1, 'optional': 'my_value2'},
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'}
            ],
            self._model.get_all(order_by=[sqlalchemy.asc(self._model.mandatory),
                                                   sqlalchemy.desc(self._model.key)]))

    def test_get_with_filter_is_retrieving_the_proper_row_after_multiple_posts(self):
        self._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual({'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                         self._model.get(optional='my_value1'))

    def test_get_with_filter_is_retrieving_the_proper_row(self):
        self._model.add_all([
            {
                'key': 'my_key1',
                'mandatory': 1,
                'optional': 'my_value1',
            },
            {
                'key': 'my_key2',
                'mandatory': 2,
                'optional': 'my_value2',
            }
        ])
        self.assertEqual({'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                         self._model.get(optional='my_value1'))

    def test_update_is_updating(self):
        self._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.assertEqual(
            (
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'},
            ),
            self._model.update({
                'key': 'my_key1',
                'optional': 'my_value',
            })
        )
        self.assertEqual({'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'},
                         self._model.get(mandatory=1))

    def test_update_is_updating_and_previous_value_cannot_be_used_to_filter(self):
        self._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self._model.update({
            'key': 'my_key1',
            'optional': 'my_value',
        })
        self.assertEqual({}, self._model.get(optional='my_value1'))

    def test_remove_with_filter_is_removing_the_proper_row(self):
        self._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(1, self._model.remove(key='my_key1'))
        self.assertEqual([{'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}],
                         self._model.get_all())

    def test_remove_without_filter_is_removing_everything(self):
        self._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(2, self._model.remove())
        self.assertEqual([], self._model.get_all())


class SQLAlchemyCRUDControllerTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    class TestAutoIncrementController(database.CRUDController):
        pass

    class TestDateController(database.CRUDController):
        pass

    _db = None

    @classmethod
    def setUpClass(cls):
        cls._db = database.load('sqlite:///:memory:', cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        class TestAutoIncrementModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = 'auto_increment_table_name'

            key = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
            enum_field = sqlalchemy.Column(sqlalchemy.Enum('Value1', 'Value2'), nullable=False,
                                           doc='Test Documentation')
            optional_with_default = sqlalchemy.Column(sqlalchemy.String, default='Test value')

        class TestDateModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = 'date_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            date_str = sqlalchemy.Column(sqlalchemy.Date)
            datetime_str = sqlalchemy.Column(sqlalchemy.DateTime)

        logger.info('Save model class...')
        cls.TestController.model(TestModel)
        cls.TestAutoIncrementController.model(TestAutoIncrementModel)
        cls.TestDateController.model(TestDateModel)
        return [TestModel, TestAutoIncrementModel, TestDateModel]

    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')
        database.reset(self._db)

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_get_all_without_data_returns_empty_list(self):
        self.assertEqual([], self.TestController.get({}))

    def test_post_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_post_list_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_post_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_post_many_with_empty_list_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([])
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_put_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.put(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_put_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.put({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_delete_without_nothing_do_not_fail(self):
        self.assertEqual(0, self.TestController.delete({}))

    def test_post_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({
                'key': 'my_key',
            })
        self.assertEqual({'mandatory': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'key': 'my_key'}, cm.exception.received_data)

    def test_post_many_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{
                'key': 'my_key',
            }])
        self.assertEqual({0: {'mandatory': ['Missing data for required field.']}}, cm.exception.errors)
        self.assertEqual([{'key': 'my_key'}], cm.exception.received_data)

    def test_post_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'mandatory': 1}, cm.exception.received_data)

    def test_post_many_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{
                'mandatory': 1,
            }])
        self.assertEqual({0: {'key': ['Missing data for required field.']}}, cm.exception.errors)
        self.assertEqual([{'mandatory': 1}], cm.exception.received_data)

    def test_post_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({
                'key': 256,
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Not a valid string.']}, cm.exception.errors)
        self.assertEqual({'key': 256, 'mandatory': 1}, cm.exception.received_data)

    def test_post_many_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{
                'key': 256,
                'mandatory': 1,
            }])
        self.assertEqual({0: {'key': ['Not a valid string.']}}, cm.exception.errors)
        self.assertEqual([{'key': 256, 'mandatory': 1}], cm.exception.received_data)

    def test_put_with_wrong_type_is_invalid(self):
        self.TestController.post({
            'key': 'value1',
            'mandatory': 1,
        })
        with self.assertRaises(Exception) as cm:
            self.TestController.put({
                'key': 'value1',
                'mandatory': 'invalid value',
            })
        self.assertEqual({'mandatory': ['Not a valid integer.']}, cm.exception.errors)
        self.assertEqual({'key': 'value1', 'mandatory': 'invalid value'}, cm.exception.received_data)

    def test_post_without_optional_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': None},
            self.TestController.post({
                'key': 'my_key',
                'mandatory': 1,
            })
        )

    def test_post_many_without_optional_is_valid(self):
        self.assertEqual(
            [
                {'mandatory': 1, 'key': 'my_key', 'optional': None},
                {'mandatory': 2, 'key': 'my_key2', 'optional': None},
            ],
            self.TestController.post_many([
                {
                    'key': 'my_key',
                    'mandatory': 1,
                },
                {
                    'key': 'my_key2',
                    'mandatory': 2,
                }
            ])
        )

    def test_post_with_optional_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
            self.TestController.post({
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
            })
        )

    def test_post_many_with_optional_is_valid(self):
        self.assertListEqual(
            [
                {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
                {'mandatory': 2, 'key': 'my_key2', 'optional': 'my_value2'},
            ],
            self.TestController.post_many([
                {
                    'key': 'my_key',
                    'mandatory': 1,
                    'optional': 'my_value',
                },
                {
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                }
            ])
        )

    def test_post_with_unknown_field_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
            self.TestController.post({
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
                # This field do not exists in schema
                'unknown': 'my_value',
            })
        )

    def test_post_many_with_unknown_field_is_valid(self):
        self.assertListEqual(
            [
                {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
                {'mandatory': 2, 'key': 'my_key2', 'optional': 'my_value2'},
            ],
            self.TestController.post_many([
                {
                    'key': 'my_key',
                    'mandatory': 1,
                    'optional': 'my_value',
                    # This field do not exists in schema
                    'unknown': 'my_value',
                },
                {
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                    # This field do not exists in schema
                    'unknown': 'my_value2',
                },
            ])
        )

    def test_post_with_specified_incremented_field_is_ignored_and_valid(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                from collections import OrderedDict
                test_fields = [name for name, model in fields.items()]
                property_list = []
                for field_name in test_fields:
                    if hasattr(fields[field_name], 'readonly') and fields[field_name].readonly:
                        property_list.append(tuple((field_name, {'readOnly': True})))
                    else:
                        property_list.append(tuple((field_name, {})))

                model = lambda: None
                setattr(model, '_schema', {'properties': OrderedDict(property_list)})
                return model

        self.TestAutoIncrementController.namespace(TestAPI)
        self.assertEqual(
            {'optional_with_default': 'Test value', 'key': 1, 'enum_field': 'Value1'},
            self.TestAutoIncrementController.post({
                'key': 'my_key',
                'enum_field': 'Value1',
            })
        )

    def test_post_many_with_specified_incremented_field_is_ignored_and_valid(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                from collections import OrderedDict
                test_fields = [name for name, model in fields.items()]
                property_list = []
                for field_name in test_fields:
                    if hasattr(fields[field_name], 'readonly') and fields[field_name].readonly:
                        property_list.append(tuple((field_name, {'readOnly': True})))
                    else:
                        property_list.append(tuple((field_name, {})))

                model = lambda: None
                setattr(model, '_schema', {'properties': OrderedDict(property_list)})
                return model

        self.TestAutoIncrementController.namespace(TestAPI)

        self.assertListEqual(
            [
                {'optional_with_default': 'Test value', 'enum_field': 'Value1', 'key': 1},
                {'optional_with_default': 'Test value', 'enum_field': 'Value2', 'key': 2},
            ],
            self.TestAutoIncrementController.post_many([{
                'key': 'my_key',
                'enum_field': 'Value1',
            }, {
                'key': 'my_key',
                'enum_field': 'Value2',
            }])
        )

    def test_get_without_filter_is_retrieving_the_only_item(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.assertEqual(
            [{
                'mandatory': 1,
                'optional': 'my_value1',
                'key': 'my_key1'
            }],
            self.TestController.get({}))

    def test_get_from_another_thread_than_post(self):
        def save_get_result():
            self.thread_get_result = self.TestController.get({})

        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })

        self.thread_get_result = None
        get_thread = Thread(name='GetInOtherThread', target=save_get_result)
        get_thread.start()
        get_thread.join()

        self.assertEqual(
            [{
                'mandatory': 1,
                'optional': 'my_value1',
                'key': 'my_key1'
            }],
            self.thread_get_result)

    def test_get_without_filter_is_retrieving_everything_with_multiple_posts(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}
            ],
            self.TestController.get({}))

    def test_get_without_filter_is_retrieving_everything(self):
        self.TestController.post_many([
            {
                'key': 'my_key1',
                'mandatory': 1,
                'optional': 'my_value1',
            },
            {
                'key': 'my_key2',
                'mandatory': 2,
                'optional': 'my_value2',
            },
        ])
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}
            ],
            self.TestController.get({}))

    def test_get_with_filter_is_retrieving_subset_with_multiple_posts(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
            ],
            self.TestController.get({'optional': 'my_value1'}))

    def test_get_with_filter_is_retrieving_subset(self):
        self.TestController.post_many([
            {
                'key': 'my_key1',
                'mandatory': 1,
                'optional': 'my_value1',
            },
            {
                'key': 'my_key2',
                'mandatory': 2,
                'optional': 'my_value2',
            },
        ])
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
            ],
            self.TestController.get({'optional': 'my_value1'}))

    def test_put_is_updating(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.assertEqual(
            (
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'},
            ),
            self.TestController.put({
                'key': 'my_key1',
                'optional': 'my_value',
            })
        )
        self.assertEqual([{'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'}],
                         self.TestController.get({'mandatory': 1}))

    def test_put_is_updating_date(self):
        self.TestDateController.post({
            'key': 'my_key1',
            'date_str': '2017-05-15',
            'datetime_str': '2016-09-23T23:59:59',
        })
        self.assertEqual(
            (
                {'date_str': '2017-05-15', 'datetime_str': '2016-09-23T23:59:59+00:00', 'key': 'my_key1'},
                {'date_str': '2018-06-01', 'datetime_str': '1989-12-31T01:00:00+00:00', 'key': 'my_key1'},
            ),
            self.TestDateController.put({
                'key': 'my_key1',
                'date_str': '2018-06-01',
                'datetime_str': '1989-12-31T01:00:00',
            })
        )
        self.assertEqual([
            {'date_str': '2018-06-01', 'datetime_str': '1989-12-31T01:00:00+00:00', 'key': 'my_key1'}
        ],
            self.TestDateController.get({'date_str': '2018-06-01'}))

    def test_get_date_is_handled_for_valid_date(self):
        self.TestDateController.post({
            'key': 'my_key1',
            'date_str': '2017-05-15',
            'datetime_str': '2016-09-23T23:59:59',
        })
        d = datetime.datetime.strptime('2017-05-15', '%Y-%m-%d').date()
        self.assertEqual(
            [
                {'date_str': '2017-05-15', 'datetime_str': '2016-09-23T23:59:59+00:00', 'key': 'my_key1'},
            ],
            self.TestDateController.get({
                'date_str': d,
            })
        )

    def test_get_date_is_handled_for_unused_date(self):
        self.TestDateController.post({
            'key': 'my_key1',
            'date_str': '2017-05-15',
            'datetime_str': '2016-09-23T23:59:59',
        })
        d = datetime.datetime.strptime('2016-09-23', '%Y-%m-%d').date()
        self.assertEqual(
            [],
            self.TestDateController.get({
                'date_str': d,
            })
        )

    def test_get_date_is_handled_for_valid_datetime(self):
        self.TestDateController.post({
            'key': 'my_key1',
            'date_str': '2017-05-15',
            'datetime_str': '2016-09-23T23:59:59',
        })
        dt = datetime.datetime.strptime('2016-09-23T23:59:59', '%Y-%m-%dT%H:%M:%S')
        self.assertEqual(
            [
                {'date_str': '2017-05-15', 'datetime_str': '2016-09-23T23:59:59+00:00', 'key': 'my_key1'},
            ],
            self.TestDateController.get({
                'datetime_str': dt,
            })
        )

    def test_get_date_is_handled_for_unused_datetime(self):
        self.TestDateController.post({
            'key': 'my_key1',
            'date_str': '2017-05-15',
            'datetime_str': '2016-09-23T23:59:59',
        })
        dt = datetime.datetime.strptime('2016-09-24T23:59:59', '%Y-%m-%dT%H:%M:%S')
        self.assertEqual(
            [],
            self.TestDateController.get({
                'datetime_str': dt,
            })
        )

    def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.put({
            'key': 'my_key1',
            'optional': 'my_value',
        })
        self.assertEqual([], self.TestController.get({'optional': 'my_value1'}))

    def test_delete_with_filter_is_removing_the_proper_row(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(1, self.TestController.delete({'key': 'my_key1'}))
        self.assertEqual([{'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}],
                         self.TestController.get({}))

    def test_delete_without_filter_is_removing_everything(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(2, self.TestController.delete({}))
        self.assertEqual([], self.TestController.get({}))

    def test_query_get_parser(self):
        self.assertEqual(
            {
                'key': str,
                'mandatory': int,
                'optional': str,
                'limit': inputs.positive,
                'offset': inputs.natural,
            },
            {arg.name: arg.type for arg in self.TestController.query_get_parser.args})

    def test_query_delete_parser(self):
        self.assertEqual(
            {
                'key': str,
                'mandatory': int,
                'optional': str,
            },
            {arg.name: arg.type for arg in self.TestController.query_delete_parser.args})

    def test_json_post_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name, field in fields.items()]
                test_fields.sort()
                test_defaults = [field.default for field in fields.values() if
                                 hasattr(field, 'default') and field.default]
                return name, test_fields, test_defaults

        self.TestController.namespace(TestAPI)
        self.assertEqual(
            ('TestModel', ['key', 'mandatory', 'optional'], []),
            self.TestController.json_post_model
        )

    def test_json_post_model_with_auto_increment_and_enum(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name, field in fields.items()]
                test_fields.sort()
                test_defaults = [field.default for field in fields.values() if
                                 hasattr(field, 'default') and field.default]
                return name, test_fields, test_defaults

        self.TestAutoIncrementController.namespace(TestAPI)
        self.assertEqual(
            ('TestAutoIncrementModel', ['enum_field', 'key', 'optional_with_default'], ['Test value']),
            self.TestAutoIncrementController.json_post_model
        )

    def test_json_put_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name, field in fields.items()]
                test_fields.sort()
                return name, test_fields

        self.TestController.namespace(TestAPI)
        self.assertEqual(
            ('TestModel', ['key', 'mandatory', 'optional']),
            self.TestController.json_put_model
        )

    def test_json_put_model_with_auto_increment_and_enum(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name, field in fields.items()]
                test_fields.sort()
                return name, test_fields

        self.TestAutoIncrementController.namespace(TestAPI)
        self.assertEqual(
            ('TestAutoIncrementModel', ['enum_field', 'key', 'optional_with_default']),
            self.TestAutoIncrementController.json_put_model
        )

    def test_get_response_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name in fields.keys()]
                test_fields.sort()
                return name, test_fields

        self.TestController.namespace(TestAPI)
        self.assertEqual(
            ('TestModel', ['key', 'mandatory', 'optional']),
            self.TestController.get_response_model)

    def test_get_response_model_with_enum(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name in fields.keys()]
                test_fields.sort()
                test_descriptions = [field.description for field in fields.values() if field.description]
                test_descriptions.sort()
                test_enums = [field.enum for field in fields.values() if hasattr(field, 'enum') and field.enum]
                return name, test_fields, test_descriptions, test_enums

        self.TestAutoIncrementController.namespace(TestAPI)
        self.assertEqual(
            (
                'TestAutoIncrementModel',
                ['enum_field', 'key', 'optional_with_default'],
                ['Test Documentation'],
                [['Value1', 'Value2']]
            ),
            self.TestAutoIncrementController.get_response_model)

    def test_get_with_limit_2_is_retrieving_subset_of_2_first_elements(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.TestController.post({
            'key': 'my_key3',
            'mandatory': 3,
            'optional': 'my_value3',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'},
            ],
            self.TestController.get({'limit': 2}))

    def test_get_with_offset_1_is_retrieving_subset_of_n_minus_1_first_elements(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.TestController.post({
            'key': 'my_key3',
            'mandatory': 3,
            'optional': 'my_value3',
        })
        self.assertEqual(
            [
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'},
                {'key': 'my_key3', 'mandatory': 3, 'optional': 'my_value3'},
            ],
            self.TestController.get({'offset': 1}))

    def test_get_with_limit_1_and_offset_1_is_retrieving_middle_element(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.TestController.post({
            'key': 'my_key3',
            'mandatory': 3,
            'optional': 'my_value3',
        })
        self.assertEqual(
            [
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'},

            ],
            self.TestController.get({'offset': 1, 'limit': 1}))

    def test_get_model_description_returns_description(self):
        self.assertEqual(
            {
                'key': 'key',
                'mandatory': 'mandatory',
                'optional': 'optional',
                'table': 'sample_table_name'
            },
            self.TestController.get_model_description())

    def test_get_model_description_response_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name in fields.keys()]
                test_fields.sort()
                return name, test_fields

        self.TestController.namespace(TestAPI)
        self.assertEqual(
            ('TestModelDescription', ['key', 'mandatory', 'optional', 'table']),
            self.TestController.get_model_description_response_model)


class SQLAlchemyCRUDControllerFailuresTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    _db = None

    @classmethod
    def setUpClass(cls):
        cls._db = database.load('sqlite:///:memory:', cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        logger.info('Save model class...')
        return [TestModel]

    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')
        database.reset(self._db)

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_namespace_method_without_setting_model(self):
        class TestNamespace:
            pass

        with self.assertRaises(Exception) as cm:
            self.TestController.namespace(TestNamespace)
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.")

    def test_get_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.get({})
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.")

    def test_post_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.get({})
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.")

    def test_post_many_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([])
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.")

    def test_put_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.put({})
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.")

    def test_delete_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.delete({})
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.")

    def test_audit_method_without_setting_model(self):
        self.assertEqual([], self.TestController.get_audit({}))

    def test_model_description_method_without_setting_model(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.get_model_description()
        self.assertRegex(
            cm.exception.args[0],
            "Model was not attached to TestController. "
            "Call <bound method CRUDController.model of <class '.*CRUDControllerFailuresTest.TestController'>>.")


class SQLAlchemyCRUDControllerAuditTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    _db = None

    @classmethod
    def setUpClass(cls):
        cls._db = database.load('sqlite:///:memory:', cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        logger.info('Save model class...')
        cls.TestController.model(TestModel, audit=True)
        return [TestModel]

    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')
        database.reset(self._db)

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_get_all_without_data_returns_empty_list(self):
        self.assertEqual([], self.TestController.get({}))
        self._check_audit([])

    def test_get_parser_fields_order(self):
        self.assertEqual(
            [
                'key',
                'mandatory',
                'optional',
                'limit',
                'offset',
            ],
            [arg.name for arg in self.TestController.query_get_parser.args]
        )

    def test_delete_parser_fields_order(self):
        self.assertEqual(
            [
                'key',
                'mandatory',
                'optional',
            ],
            [arg.name for arg in self.TestController.query_delete_parser.args]
        )

    def test_post_model_fields_order(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                return list(fields.keys())

        self.TestController.namespace(TestAPI)
        self.assertEqual(
            [
                'key',
                'mandatory',
                'optional',
            ],
            self.TestController.json_post_model
        )

    def test_put_model_fields_order(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                return list(fields.keys())

        self.TestController.namespace(TestAPI)
        self.assertEqual(
            [
                'key',
                'mandatory',
                'optional',
            ],
            self.TestController.json_put_model
        )

    def test_get_response_model_fields_order(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                return list(fields.keys())

        self.TestController.namespace(TestAPI)
        self.assertEqual(
            [
                'key',
                'mandatory',
                'optional',
            ],
            self.TestController.get_response_model
        )

    def test_post_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit([])

    def test_post_many_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit([])

    def test_post_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit([])

    def test_post_many_with_empty_list_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([])
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit([])

    def test_put_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.put(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit([])

    def test_put_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.put({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit([])

    def test_delete_without_nothing_do_not_fail(self):
        self.assertEqual(0, self.TestController.delete({}))
        self._check_audit([])

    def test_post_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({
                'key': 'my_key',
            })
        self.assertEqual({'mandatory': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'key': 'my_key'}, cm.exception.received_data)
        self._check_audit([])

    def test_post_many_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{
                'key': 'my_key',
            }])
        self.assertEqual({0: {'mandatory': ['Missing data for required field.']}}, cm.exception.errors)
        self.assertEqual([{'key': 'my_key'}], cm.exception.received_data)
        self._check_audit([])

    def test_post_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'mandatory': 1}, cm.exception.received_data)
        self._check_audit([])

    def test_post_many_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{
                'mandatory': 1,
            }])
        self.assertEqual({0: {'key': ['Missing data for required field.']}}, cm.exception.errors)
        self.assertEqual([{'mandatory': 1}], cm.exception.received_data)
        self._check_audit([])

    def test_post_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post({
                'key': 256,
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Not a valid string.']}, cm.exception.errors)
        self.assertEqual({'key': 256, 'mandatory': 1}, cm.exception.received_data)
        self._check_audit([])

    def test_post_many_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            self.TestController.post_many([{
                'key': 256,
                'mandatory': 1,
            }])
        self.assertEqual({0: {'key': ['Not a valid string.']}}, cm.exception.errors)
        self.assertEqual([{'key': 256, 'mandatory': 1}], cm.exception.received_data)
        self._check_audit([])

    def test_put_with_wrong_type_is_invalid(self):
        self.TestController.post({
            'key': 'value1',
            'mandatory': 1,
        })
        with self.assertRaises(Exception) as cm:
            self.TestController.put({
                'key': 'value1',
                'mandatory': 'invalid_value',
            })
        self.assertEqual({'mandatory': ['Not a valid integer.']}, cm.exception.errors)
        self.assertEqual({'key': 'value1', 'mandatory': 'invalid_value'}, cm.exception.received_data)
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'value1',
                    'mandatory': 1,
                    'optional': None,
                },
            ]
        )

    def test_post_without_optional_is_valid(self):
        self.assertEqual(
            {'optional': None, 'mandatory': 1, 'key': 'my_key'},
            self.TestController.post({
                'key': 'my_key',
                'mandatory': 1,
            })
        )
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': None,
                },
            ]
        )

    def test_post_many_without_optional_is_valid(self):
        self.assertListEqual(
            [{'optional': None, 'mandatory': 1, 'key': 'my_key'}],
            self.TestController.post_many([{
                'key': 'my_key',
                'mandatory': 1,
            }])
        )
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': None,
                },
            ]
        )

    def _check_audit(self, expected_audit, filter_audit={}):
        audit = self.TestController.get_audit(filter_audit)
        audit = [{key: audit_line[key] for key in sorted(audit_line.keys())} for audit_line in audit]

        if not expected_audit:
            self.assertEqual(audit, expected_audit)
        else:
            self.assertRegex(f'{audit}', f'{expected_audit}')

    def test_post_with_optional_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
            self.TestController.post({
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
            })
        )
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key',
                    'mandatory': 1,
                    'optional': 'my_value',
                }
            ]
        )

    def test_post_many_with_optional_is_valid(self):
        self.assertListEqual(
            [{'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'}],
            self.TestController.post_many([{
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
            }])
        )
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key',
                    'mandatory': 1,
                    'optional': 'my_value',
                }
            ]
        )

    def test_post_with_unknown_field_is_valid(self):
        self.assertEqual(
            {'optional': 'my_value', 'mandatory': 1, 'key': 'my_key'},
            self.TestController.post({
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
                # This field do not exists in schema
                'unknown': 'my_value',
            })
        )
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key',
                    'mandatory': 1,
                    'optional': 'my_value',
                },
            ]
        )

    def test_post_many_with_unknown_field_is_valid(self):
        self.assertListEqual(
            [{'optional': 'my_value', 'mandatory': 1, 'key': 'my_key'}],
            self.TestController.post_many([{
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
                # This field do not exists in schema
                'unknown': 'my_value',
            }])
        )
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key',
                    'mandatory': 1,
                    'optional': 'my_value',
                },
            ]
        )

    def test_get_without_filter_is_retrieving_the_only_item(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.assertEqual(
            [{
                'mandatory': 1,
                'optional': 'my_value1',
                'key': 'my_key1'
            }],
            self.TestController.get({}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
            ]
        )

    def test_get_without_filter_is_retrieving_everything_with_multiple_posts(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}
            ],
            self.TestController.get({}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                },
            ]
        )

    def test_get_without_filter_is_retrieving_everything(self):
        self.TestController.post_many([
            {
                'key': 'my_key1',
                'mandatory': 1,
                'optional': 'my_value1',
            },
            {
                'key': 'my_key2',
                'mandatory': 2,
                'optional': 'my_value2',
            }
        ])
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}
            ],
            self.TestController.get({}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                },
            ]
        )

    def test_get_with_filter_is_retrieving_subset(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
            ],
            self.TestController.get({'optional': 'my_value1'}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                },
            ]
        )

    def test_put_is_updating(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.assertEqual(
            (
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'},
            ),
            self.TestController.put({
                'key': 'my_key1',
                'optional': 'my_value',
            })
        )
        self.assertEqual([{'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'}],
                         self.TestController.get({'mandatory': 1}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'U',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value',
                },
            ]
        )

    def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.put({
            'key': 'my_key1',
            'optional': 'my_value',
        })
        self.assertEqual([], self.TestController.get({'optional': 'my_value1'}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'U',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value',
                },
            ]
        )

    def test_delete_with_filter_is_removing_the_proper_row(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(1, self.TestController.delete({'key': 'my_key1'}))
        self.assertEqual([{'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}],
                         self.TestController.get({}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                },
                {
                    'audit_action': 'D',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
            ]
        )

    def test_audit_filter_on_model_is_returning_only_selected_data(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.put({
            'key': 'my_key1',
            'mandatory': 2,
        })
        self.TestController.delete({'key': 'my_key1'})
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'U',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 2,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'D',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 2,
                    'optional': 'my_value1',
                },
            ],
            filter_audit={'key': 'my_key1'}
        )

    def test_audit_filter_on_audit_model_is_returning_only_selected_data(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.put({
            'key': 'my_key1',
            'mandatory': 2,
        })
        time.sleep(1)
        self.TestController.delete({'key': 'my_key1'})
        self._check_audit(
            [
                {
                    'audit_action': 'U',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 2,
                    'optional': 'my_value1',
                },
            ],
            filter_audit={'audit_action': 'U'}
        )

    def test_delete_without_filter_is_removing_everything(self):
        self.TestController.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.TestController.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(2, self.TestController.delete({}))
        self.assertEqual([], self.TestController.get({}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                },
                {
                    'audit_action': 'D',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'D',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                },
            ]
        )

    def test_query_get_parser(self):
        self.assertEqual(
            {
                'key': str,
                'mandatory': int,
                'optional': str,
                'limit': inputs.positive,
                'offset': inputs.natural,
            },
            {arg.name: arg.type for arg in self.TestController.query_get_parser.args})
        self._check_audit([])

    def test_query_get_audit_parser(self):
        self.assertEqual(
            {
                'audit_action': str,
                'audit_date_utc': inputs.datetime_from_iso8601,
                'audit_user': str,
                'key': str,
                'mandatory': int,
                'optional': str,
                'limit': inputs.positive,
                'offset': inputs.natural,
            },
            {arg.name: arg.type for arg in self.TestController.query_get_audit_parser.args})
        self._check_audit([])

    def test_query_delete_parser(self):
        self.assertEqual(
            {
                'key': str,
                'mandatory': int,
                'optional': str,
            },
            {arg.name: arg.type for arg in self.TestController.query_delete_parser.args})
        self._check_audit([])

    def test_get_response_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name in fields.keys()]
                test_fields.sort()
                return name, test_fields

        self.TestController.namespace(TestAPI)
        self.assertEqual(
            ('TestModel', ['key', 'mandatory', 'optional']),
            self.TestController.get_response_model)
        self._check_audit([])

    def test_get_audit_response_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name in fields.keys()]
                test_fields.sort()
                return name, test_fields

        self.TestController.namespace(TestAPI)
        self.assertEqual(
            (
                'AuditTestModel', [
                    'audit_action',
                    'audit_date_utc',
                    'audit_user',
                    'key',
                    'mandatory',
                    'optional'
                ],
            ),
            self.TestController.get_audit_response_model)
        self._check_audit([])


class SQLAlchemyFlaskRestPlusModelsTest(unittest.TestCase):
    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_rest_plus_type_for_string_field_is_string(self):
        field = marshmallow_fields.String()
        self.assertEqual(flask_rest_plus_fields.String, database_sqlalchemy._get_rest_plus_type(field))

    def test_rest_plus_type_for_int_field_is_integer(self):
        field = marshmallow_fields.Integer()
        self.assertEqual(flask_rest_plus_fields.Integer, database_sqlalchemy._get_rest_plus_type(field))

    def test_rest_plus_type_for_bool_field_is_boolean(self):
        field = marshmallow_fields.Boolean()
        self.assertEqual(flask_rest_plus_fields.Boolean, database_sqlalchemy._get_rest_plus_type(field))

    def test_rest_plus_type_for_date_field_is_date(self):
        field = marshmallow_fields.Date()
        self.assertEqual(flask_rest_plus_fields.Date, database_sqlalchemy._get_rest_plus_type(field))

    def test_rest_plus_type_for_datetime_field_is_datetime(self):
        field = marshmallow_fields.DateTime()
        self.assertEqual(flask_rest_plus_fields.DateTime, database_sqlalchemy._get_rest_plus_type(field))

    def test_rest_plus_type_for_decimal_field_is_fixed(self):
        field = marshmallow_fields.Decimal()
        self.assertEqual(flask_rest_plus_fields.Fixed, database_sqlalchemy._get_rest_plus_type(field))

    def test_rest_plus_type_for_float_field_is_float(self):
        field = marshmallow_fields.Float()
        self.assertEqual(flask_rest_plus_fields.Float, database_sqlalchemy._get_rest_plus_type(field))

    def test_rest_plus_type_for_number_field_is_decimal(self):
        field = marshmallow_fields.Number()
        self.assertEqual(flask_rest_plus_fields.Decimal, database_sqlalchemy._get_rest_plus_type(field))

    def test_rest_plus_type_for_time_field_is_datetime(self):
        field = marshmallow_fields.Time()
        self.assertEqual(flask_rest_plus_fields.DateTime, database_sqlalchemy._get_rest_plus_type(field))

    def test_rest_plus_type_for_field_field_is_string(self):
        field = marshmallow_fields.Field()
        self.assertEqual(flask_rest_plus_fields.String, database_sqlalchemy._get_rest_plus_type(field))

    def test_rest_plus_type_for_none_field_cannot_be_guessed(self):
        with self.assertRaises(Exception) as cm:
            database_sqlalchemy._get_rest_plus_type(None)
        self.assertEqual('Flask RestPlus field type cannot be guessed for None field.', cm.exception.args[0])

    def test_rest_plus_example_for_string_field(self):
        field = marshmallow_fields.String()
        self.assertEqual('sample_value', database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_int_field_is_integer(self):
        field = marshmallow_fields.Integer()
        self.assertEqual('0', database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_bool_field_is_true(self):
        field = marshmallow_fields.Boolean()
        self.assertEqual('true', database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_date_field_is_YYYY_MM_DD(self):
        field = marshmallow_fields.Date()
        self.assertEqual('2017-09-24', database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_datetime_field_is_YYYY_MM_DDTHH_MM_SS(self):
        field = marshmallow_fields.DateTime()
        self.assertEqual('2017-09-24T15:36:09', database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_decimal_field_is_decimal(self):
        field = marshmallow_fields.Decimal()
        self.assertEqual('0.0', database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_float_field_is_float(self):
        field = marshmallow_fields.Float()
        self.assertEqual('0.0', database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_number_field_is_decimal(self):
        field = marshmallow_fields.Number()
        self.assertEqual('0.0', database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_time_field_is_HH_MM_SS(self):
        field = marshmallow_fields.Time()
        self.assertEqual('15:36:09', database_sqlalchemy._get_example(field))

    def test_rest_plus_example_for_none_field_is_sample_value(self):
        self.assertEqual('sample_value', database_sqlalchemy._get_example(None))


class SQlAlchemyColumnsTest(unittest.TestCase):
    _model = None

    @classmethod
    def setUpClass(cls):
        database.load('sqlite:///:memory:', cls._create_models)

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database_sqlalchemy.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            string_column = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            integer_column = sqlalchemy.Column(sqlalchemy.Integer)
            boolean_column = sqlalchemy.Column(sqlalchemy.Boolean)
            date_column = sqlalchemy.Column(sqlalchemy.Date)
            datetime_column = sqlalchemy.Column(sqlalchemy.DateTime)
            float_column = sqlalchemy.Column(sqlalchemy.Float)

        logger.info('Save model class...')
        cls._model = TestModel
        return [TestModel]

    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_field_declaration_order_is_kept_in_schema(self):
        fields = self._model.schema().fields
        self.assertEqual(
            [
                'string_column',
                'integer_column',
                'boolean_column',
                'date_column',
                'datetime_column',
                'float_column',
            ],
            [field_name for field_name in fields]
        )

    def test_python_type_for_sqlalchemy_string_field_is_string(self):
        field = self._model.schema().fields['string_column']
        self.assertEqual(str, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_integer_field_is_integer(self):
        field = self._model.schema().fields['integer_column']
        self.assertEqual(int, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_boolean_field_is_boolean(self):
        field = self._model.schema().fields['boolean_column']
        self.assertEqual(inputs.boolean, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_date_field_is_date(self):
        field = self._model.schema().fields['date_column']
        self.assertEqual(inputs.date_from_iso8601, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_datetime_field_is_datetime(self):
        field = self._model.schema().fields['datetime_column']
        self.assertEqual(inputs.datetime_from_iso8601, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_float_field_is_float(self):
        field = self._model.schema().fields['float_column']
        self.assertEqual(float, database_sqlalchemy._get_python_type(field))

    def test_python_type_for_sqlalchemy_none_field_cannot_be_guessed(self):
        with self.assertRaises(Exception) as cm:
            database_sqlalchemy._get_python_type(None)
        self.assertEqual('Python field type cannot be guessed for None field.', cm.exception.args[0])


class FlaskRestPlusErrorsTest(unittest.TestCase):
    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_handle_exception_failed_validation_on_list_of_items(self):
        class TestAPI:

            @staticmethod
            def model(cls, name):
                pass

            @staticmethod
            def errorhandler(cls):
                pass

            @staticmethod
            def marshal_with(cls, code):
                def wrapper(func):
                    # Mock of the input (List of items)
                    received_data = [{'optional_string_value': 'my_value1', 'mandatory_integer_value': 1,
                                      'optional_enum_value': 'First Enum Value', 'optional_date_value': '2017-10-23',
                                      'optional_date_time_value': '2017-10-24T21:46:57.12458+00:00',
                                      'optional_float_value': 100},
                                     {'optional_string_value': 'my_value2', 'optional_enum_value': 'First Enum Value',
                                      'optional_date_value': '2017-10-23',
                                      'optional_date_time_value': '2017-10-24T21:46:57.12458+00:00',
                                      'optional_float_value': 200}]
                    failed_validation = flask_restplus_errors.ValidationFailed(received_data)
                    errors = {1: {'mandatory_integer_value': ['Missing data for required field.']}}
                    setattr(failed_validation, 'errors', errors)
                    # Call handle_exception method
                    result = func(failed_validation)
                    # Assert output, if NOK will raise Assertion Error
                    assert (result[0] == {'fields': [{'field_name': 'mandatory_integer_value on item number: 1.',
                                                      'messages': ['Missing data for required field.']}]})
                    assert (result[1] == 400)
                    return result

                return wrapper

        # Since TestApi is not completely mocked, add_failed_validation_handler will raise a "NoneType is not iterable
        # exception after the call to handle_exception. Exception is therefore ignored
        try:
            flask_restplus_errors.add_failed_validation_handler(TestAPI)
        except TypeError:
            pass

    def test_handle_exception_failed_validation_a_single_item(self):
        class TestAPI:

            @staticmethod
            def model(cls, name):
                pass

            @staticmethod
            def errorhandler(cls):
                pass

            @staticmethod
            def marshal_with(cls, code):
                def wrapper(func):
                    # Mock of the input (Single item)
                    received_data = {'optional_string_value': 'my_value1', 'mandatory_integer_value': 1,
                                     'optional_enum_value': 'First Enum Value', 'optional_date_value': '2017-10-23',
                                     'optional_date_time_value': '2017-10-24T21:46:57.12458+00:00',
                                     'optional_float_value': 100}, {'optional_string_value': 'my_value2',
                                                                    'optional_enum_value': 'First Enum Value',
                                                                    'optional_date_value': '2017-10-23',
                                                                    'optional_date_time_value': '2017-10-24T21:46:57.12458+00:00',
                                                                    'optional_float_value': 200}
                    failed_validation = flask_restplus_errors.ValidationFailed(received_data)
                    errors = {'mandatory_integer_value': ['Missing data for required field.']}
                    setattr(failed_validation, 'errors', errors)
                    # Call handle_exception method
                    result = func(failed_validation)
                    # Assert output, if NOK will raise Assertion Error
                    assert (result[0] == {'fields': [
                        {'field_name': 'mandatory_integer_value', 'messages': ['Missing data for required field.']}]})
                    assert (result[1] == 400)
                    return result

                return wrapper

        # Since TestApi is not completely mocked, add_failed_validation_handler will raise a "NoneType is not iterable
        # exception after the call to handle_exception. Exception is therefore ignored
        try:
            flask_restplus_errors.add_failed_validation_handler(TestAPI)
        except TypeError:
            pass


if __name__ == '__main__':
    unittest.main()
