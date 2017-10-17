import unittest
import sqlalchemy
import logging
import sys
import datetime
from marshmallow_sqlalchemy.fields import fields as marshmallow_fields
from flask_restplus import fields as flask_rest_plus_fields

logging.basicConfig(
    format='%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.DEBUG)

from pycommon_database import database, flask_restplus_errors, flask_restplus_models

logger = logging.getLogger(__name__)


class DatabaseTest(unittest.TestCase):
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
                         database._clean_database_url('sybase+pyodbc:///?odbc_connect=TEST=VALUE;TEST2=VALUE2'))

    def test_sybase_does_not_support_offset(self):
        self.assertFalse(database._supports_offset('sybase+pyodbc'))

    def test_sql_lite_support_offset(self):
        self.assertTrue(database._supports_offset('sqlite'))


class CRUDModelTest(unittest.TestCase):
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

        class TestModel(database.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        logger.info('Save model class...')
        cls._model = TestModel
        return [TestModel]

    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')
        database.reset(CRUDModelTest._db)

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_get_all_without_data_returns_empty_list(self):
        self.assertEqual([], CRUDModelTest._model.get_all())

    def test_get_without_data_returns_empty_dict(self):
        self.assertEqual({}, CRUDModelTest._model.get())

    def test_add_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.add(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_add_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.add({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_update_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.update(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_update_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.update({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_remove_without_nothing_do_not_fail(self):
        self.assertEqual(0, CRUDModelTest._model.remove())

    def test_add_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.add({
                'key': 'my_key',
            })
        self.assertEqual({'mandatory': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'key': 'my_key'}, cm.exception.received_data)

    def test_add_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.add({
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'mandatory': 1}, cm.exception.received_data)

    def test_add_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.add({
                'key': 256,
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Not a valid string.']}, cm.exception.errors)
        self.assertEqual({'key': 256, 'mandatory': 1}, cm.exception.received_data)

    def test_update_with_wrong_type_is_invalid(self):
        CRUDModelTest._model.add({
            'key': 'value1',
            'mandatory': 1,
        })
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.update({
                'key': 'value1',
                'mandatory': 'invalid_value',
            })
        self.assertEqual({'mandatory': ['Not a valid integer.']}, cm.exception.errors)
        self.assertEqual({'key': 'value1', 'mandatory': 'invalid_value'}, cm.exception.received_data)

    def test_add_without_optional_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': None},
            CRUDModelTest._model.add({
                'key': 'my_key',
                'mandatory': 1,
            })
        )

    def test_add_with_optional_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
            CRUDModelTest._model.add({
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
            })
        )

    def test_add_with_unknown_field_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
            CRUDModelTest._model.add({
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
                # This field do not exists in schema
                'unknown': 'my_value',
            })
        )

    def test_get_without_filter_is_retrieving_the_only_item(self):
        CRUDModelTest._model.add({
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
            CRUDModelTest._model.get())

    def test_get_without_filter_is_failing_if_more_than_one_item_exists(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.get()
        self.assertEqual({'': ['More than one result: Consider another filtering.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_get_all_without_filter_is_retrieving_everything(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}
            ],
            CRUDModelTest._model.get_all())

    def test_get_all_with_filter_is_retrieving_subset(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
            ],
            CRUDModelTest._model.get_all(optional='my_value1'))

    def test_get_with_filter_is_retrieving_the_proper_row(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual({'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                         CRUDModelTest._model.get(optional='my_value1'))

    def test_update_is_updating(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.assertEqual(
            (
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'},
            ),
            CRUDModelTest._model.update({
                'key': 'my_key1',
                'optional': 'my_value',
            })
        )
        self.assertEqual({'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'},
                         CRUDModelTest._model.get(mandatory=1))

    def test_update_is_updating_and_previous_value_cannot_be_used_to_filter(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDModelTest._model.update({
            'key': 'my_key1',
            'optional': 'my_value',
        })
        self.assertEqual({}, CRUDModelTest._model.get(optional='my_value1'))

    def test_remove_with_filter_is_removing_the_proper_row(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(1, CRUDModelTest._model.remove(key='my_key1'))
        self.assertEqual([{'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}],
                         CRUDModelTest._model.get_all())

    def test_remove_without_filter_is_removing_everything(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(2, CRUDModelTest._model.remove())
        self.assertEqual([], CRUDModelTest._model.get_all())


class CRUDControllerTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    class TestAutoIncrementController(database.CRUDController):
        pass

    class TestDateController(database.CRUDController):
        pass

    _db = None
    _controller = TestController()
    _controller_auto_increment = TestAutoIncrementController()
    _date_controller = TestDateController()

    @classmethod
    def setUpClass(cls):
        cls._db = database.load('sqlite:///:memory:', cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        class TestAutoIncrementModel(database.CRUDModel, base):
            __tablename__ = 'auto_increment_table_name'

            key = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
            enum_field = sqlalchemy.Column(sqlalchemy.Enum('Value1', 'Value2'), nullable=False,
                                           doc='Test Documentation')
            optional_with_default = sqlalchemy.Column(sqlalchemy.String, default='Test value')

        class TestDateModel(database.CRUDModel, base):
            __tablename__ = 'date_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            date_str = sqlalchemy.Column(sqlalchemy.Date)
            datetime_str = sqlalchemy.Column(sqlalchemy.DateTime)

        logger.info('Save model class...')
        cls._controller.model(TestModel)
        cls._controller_auto_increment.model(TestAutoIncrementModel)
        cls._date_controller.model(TestDateModel)
        return [TestModel, TestAutoIncrementModel, TestDateModel]

    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')
        database.reset(CRUDControllerTest._db)

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_get_all_without_data_returns_empty_list(self):
        self.assertEqual([], CRUDControllerTest._controller.get({}))

    def test_post_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerTest._controller.post(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_post_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerTest._controller.post({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_put_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerTest._controller.put(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_put_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerTest._controller.put({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_delete_without_nothing_do_not_fail(self):
        self.assertEqual(0, CRUDControllerTest._controller.delete({}))

    def test_post_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerTest._controller.post({
                'key': 'my_key',
            })
        self.assertEqual({'mandatory': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'key': 'my_key'}, cm.exception.received_data)

    def test_post_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerTest._controller.post({
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'mandatory': 1}, cm.exception.received_data)

    def test_post_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerTest._controller.post({
                'key': 256,
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Not a valid string.']}, cm.exception.errors)
        self.assertEqual({'key': 256, 'mandatory': 1}, cm.exception.received_data)

    def test_put_with_wrong_type_is_invalid(self):
        CRUDControllerTest._controller.post({
            'key': 'value1',
            'mandatory': 1,
        })
        with self.assertRaises(Exception) as cm:
            CRUDControllerTest._controller.put({
                'key': 'value1',
                'mandatory': 'invalid value',
            })
        self.assertEqual({'mandatory': ['Not a valid integer.']}, cm.exception.errors)
        self.assertEqual({'key': 'value1', 'mandatory': 'invalid value'}, cm.exception.received_data)

    def test_post_without_optional_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': None},
            CRUDControllerTest._controller.post({
                'key': 'my_key',
                'mandatory': 1,
            })
        )

    def test_post_with_optional_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
            CRUDControllerTest._controller.post({
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
            })
        )

    def test_post_with_unknown_field_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
            CRUDControllerTest._controller.post({
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
                # This field do not exists in schema
                'unknown': 'my_value',
            })
        )

    def test_get_without_filter_is_retrieving_the_only_item(self):
        CRUDControllerTest._controller.post({
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
            CRUDControllerTest._controller.get({}))

    def test_get_without_filter_is_retrieving_everything(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}
            ],
            CRUDControllerTest._controller.get({}))

    def test_get_with_filter_is_retrieving_subset(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
            ],
            CRUDControllerTest._controller.get({'optional': 'my_value1'}))

    def test_put_is_updating(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.assertEqual(
            (
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'},
            ),
            CRUDControllerTest._controller.put({
                'key': 'my_key1',
                'optional': 'my_value',
            })
        )
        self.assertEqual([{'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'}],
                         CRUDControllerTest._controller.get({'mandatory': 1}))

    def test_put_is_updating_date(self):
        CRUDControllerTest._date_controller.post({
            'key': 'my_key1',
            'date_str': '2017-05-15',
            'datetime_str': '2016-09-23T23:59:59.123456',
        })
        self.assertEqual(
            (
                {'date_str': '2017-05-15', 'datetime_str': '2016-09-23T23:59:59.123456+00:00', 'key': 'my_key1'},
                {'date_str': '2018-06-01', 'datetime_str': '1989-12-31T01:00:00+00:00', 'key': 'my_key1'},
            ),
            CRUDControllerTest._date_controller.put({
                'key': 'my_key1',
                'date_str': '2018-06-01',
                'datetime_str': '1989-12-31T01:00:00',
            })
        )
        self.assertEqual([
            {'date_str': '2018-06-01', 'datetime_str': '1989-12-31T01:00:00+00:00', 'key': 'my_key1'}
        ],
            CRUDControllerTest._date_controller.get({'date_str': '2018-06-01'}))

    def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.put({
            'key': 'my_key1',
            'optional': 'my_value',
        })
        self.assertEqual([], CRUDControllerTest._controller.get({'optional': 'my_value1'}))

    def test_delete_with_filter_is_removing_the_proper_row(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(1, CRUDControllerTest._controller.delete({'key': 'my_key1'}))
        self.assertEqual([{'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}],
                         CRUDControllerTest._controller.get({}))

    def test_delete_without_filter_is_removing_everything(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(2, CRUDControllerTest._controller.delete({}))
        self.assertEqual([], CRUDControllerTest._controller.get({}))

    def test_query_get_parser(self):
        self.assertEqual(
            {
                'key': str,
                'mandatory': int,
                'optional': str,
                'limit': int,
                'offset': int,
            },
            {arg.name: arg.type for arg in CRUDControllerTest._controller.query_get_parser.args})

    def test_query_delete_parser(self):
        self.assertEqual(
            {
                'key': str,
                'mandatory': int,
                'optional': str,
            },
            {arg.name: arg.type for arg in CRUDControllerTest._controller.query_delete_parser.args})

    def test_json_post_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name, field in fields.items()]
                test_fields.sort()
                return name, test_fields

        CRUDControllerTest._controller.namespace(TestAPI)
        self.assertEqual(
            ('TestModel', ['key', 'mandatory', 'optional']),
            CRUDControllerTest._controller.json_post_model
        )

    def test_json_post_model_with_auto_increment_and_enum(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name, field in fields.items()]
                test_fields.sort()
                return name, test_fields

        CRUDControllerTest._controller_auto_increment.namespace(TestAPI)
        self.assertEqual(
            ('TestAutoIncrementModel', ['enum_field', 'optional_with_default']),
            CRUDControllerTest._controller_auto_increment.json_post_model
        )

    def test_json_put_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name, field in fields.items()]
                test_fields.sort()
                return name, test_fields

        CRUDControllerTest._controller.namespace(TestAPI)
        self.assertEqual(
            ('TestModel', ['key', 'mandatory', 'optional']),
            CRUDControllerTest._controller.json_put_model
        )

    def test_json_put_model_with_auto_increment_and_enum(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name, field in fields.items()]
                test_fields.sort()
                return name, test_fields

        CRUDControllerTest._controller_auto_increment.namespace(TestAPI)
        self.assertEqual(
            ('TestAutoIncrementModel', ['enum_field', 'key', 'optional_with_default']),
            CRUDControllerTest._controller_auto_increment.json_put_model
        )

    def test_get_response_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name in fields.keys()]
                test_fields.sort()
                return name, test_fields

        CRUDControllerTest._controller.namespace(TestAPI)
        self.assertEqual(
            ('TestModel', ['key', 'mandatory', 'optional']),
            CRUDControllerTest._controller.get_response_model)

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

        CRUDControllerTest._controller_auto_increment.namespace(TestAPI)
        self.assertEqual(
            (
                'TestAutoIncrementModel',
                ['enum_field', 'key', 'optional_with_default'],
                ['Test Documentation'],
                [['Value1', 'Value2']]
            ),
            CRUDControllerTest._controller_auto_increment.get_response_model)

    def test_get_with_limit_2_is_retrieving_subset_of_2_first_elements(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key3',
            'mandatory': 3,
            'optional': 'my_value3',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'},
            ],
            CRUDControllerTest._controller.get({'limit': 2}))

    def test_get_with_offset_1_is_retrieving_subset_of_n_minus_1_first_elements(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key3',
            'mandatory': 3,
            'optional': 'my_value3',
        })
        self.assertEqual(
            [
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'},
                {'key': 'my_key3', 'mandatory': 3, 'optional': 'my_value3'},
            ],
            CRUDControllerTest._controller.get({'offset': 1}))

    def test_get_with_limit_1_and_offset_1_is_retrieving_middle_element(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key3',
            'mandatory': 3,
            'optional': 'my_value3',
        })
        self.assertEqual(
            [
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'},

            ],
            CRUDControllerTest._controller.get({'offset': 1, 'limit': 1}))

    def test_get_model_description_returns_description(self):
        self.assertEqual(
            {
                'key': 'key',
                'mandatory': 'mandatory',
                'optional': 'optional',
                'table': 'sample_table_name'
            },
            CRUDControllerTest._controller.get_model_description())

    def test_get_model_description_response_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name in fields.keys()]
                test_fields.sort()
                return name, test_fields

        CRUDControllerTest._controller.namespace(TestAPI)
        self.assertEqual(
            ('TestModelDescription', ['key', 'mandatory', 'optional', 'table']),
            CRUDControllerTest._controller.get_model_description_response_model)


class CRUDControllerAuditTest(unittest.TestCase):
    class TestController(database.CRUDController):
        pass

    _db = None
    _controller = TestController()

    @classmethod
    def setUpClass(cls):
        cls._db = database.load('sqlite:///:memory:', cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        logger.info('Save model class...')
        cls._controller.model(TestModel, audit=True)
        return [TestModel]

    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')
        database.reset(CRUDControllerAuditTest._db)

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_get_all_without_data_returns_empty_list(self):
        self.assertEqual([], CRUDControllerAuditTest._controller.get({}))
        self._check_audit([])

    def test_post_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerAuditTest._controller.post(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit([])

    def test_post_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerAuditTest._controller.post({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit([])

    def test_put_with_nothing_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerAuditTest._controller.put(None)
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit([])

    def test_put_with_empty_dict_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerAuditTest._controller.put({})
        self.assertEqual({'': ['No data provided.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)
        self._check_audit([])

    def test_delete_without_nothing_do_not_fail(self):
        self.assertEqual(0, CRUDControllerAuditTest._controller.delete({}))
        self._check_audit([])

    def test_post_without_mandatory_field_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerAuditTest._controller.post({
                'key': 'my_key',
            })
        self.assertEqual({'mandatory': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'key': 'my_key'}, cm.exception.received_data)
        self._check_audit([])

    def test_post_without_key_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerAuditTest._controller.post({
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'mandatory': 1}, cm.exception.received_data)
        self._check_audit([])

    def test_post_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerAuditTest._controller.post({
                'key': 256,
                'mandatory': 1,
            })
        self.assertEqual({'key': ['Not a valid string.']}, cm.exception.errors)
        self.assertEqual({'key': 256, 'mandatory': 1}, cm.exception.received_data)
        self._check_audit([])

    def test_put_with_wrong_type_is_invalid(self):
        CRUDControllerAuditTest._controller.post({
            'key': 'value1',
            'mandatory': 1,
        })
        with self.assertRaises(Exception) as cm:
            CRUDControllerAuditTest._controller.put({
                'key': 'value1',
                'mandatory': 'invalid_value',
            })
        self.assertEqual({'mandatory': ['Not a valid integer.']}, cm.exception.errors)
        self.assertEqual({'key': 'value1', 'mandatory': 'invalid_value'}, cm.exception.received_data)
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
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
            CRUDControllerAuditTest._controller.post({
                'key': 'my_key',
                'mandatory': 1,
            })
        )
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': None,
                },
            ]
        )

    def _check_audit(self, expected_audit, filter_audit={}):
        audit = CRUDControllerAuditTest._controller.get_audit(filter_audit)
        audit = [{key: audit_line[key] for key in sorted(audit_line.keys())} for audit_line in audit]

        if not expected_audit:
            self.assertEqual(audit, expected_audit)
        else:
            self.assertRegex(f'{audit}', f'{expected_audit}')

    def test_post_with_optional_is_valid(self):
        self.assertEqual(
            {'mandatory': 1, 'key': 'my_key', 'optional': 'my_value'},
            CRUDControllerAuditTest._controller.post({
                'key': 'my_key',
                'mandatory': 1,
                'optional': 'my_value',
            })
        )
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
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
            CRUDControllerAuditTest._controller.post({
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
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key',
                    'mandatory': 1,
                    'optional': 'my_value',
                },
            ]
        )

    def test_get_without_filter_is_retrieving_the_only_item(self):
        CRUDControllerAuditTest._controller.post({
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
            CRUDControllerAuditTest._controller.get({}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
            ]
        )

    def test_get_without_filter_is_retrieving_everything(self):
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}
            ],
            CRUDControllerAuditTest._controller.get({}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                },
            ]
        )

    def test_get_with_filter_is_retrieving_subset(self):
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
            ],
            CRUDControllerAuditTest._controller.get({'optional': 'my_value1'}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                },
            ]
        )

    def test_put_is_updating(self):
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        self.assertEqual(
            (
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value1'},
                {'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'},
            ),
            CRUDControllerAuditTest._controller.put({
                'key': 'my_key1',
                'optional': 'my_value',
            })
        )
        self.assertEqual([{'key': 'my_key1', 'mandatory': 1, 'optional': 'my_value'}],
                         CRUDControllerAuditTest._controller.get({'mandatory': 1}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'U',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value',
                },
            ]
        )

    def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(self):
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerAuditTest._controller.put({
            'key': 'my_key1',
            'optional': 'my_value',
        })
        self.assertEqual([], CRUDControllerAuditTest._controller.get({'optional': 'my_value1'}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'U',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value',
                },
            ]
        )

    def test_delete_with_filter_is_removing_the_proper_row(self):
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(1, CRUDControllerAuditTest._controller.delete({'key': 'my_key1'}))
        self.assertEqual([{'key': 'my_key2', 'mandatory': 2, 'optional': 'my_value2'}],
                         CRUDControllerAuditTest._controller.get({}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                },
                {
                    'audit_action': 'D',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
            ]
        )

    def test_audit_filter_on_model_is_returning_only_selected_data(self):
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerAuditTest._controller.put({
            'key': 'my_key1',
            'mandatory': 2,
        })
        CRUDControllerAuditTest._controller.delete({'key': 'my_key1'})
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'U',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 2,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'D',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 2,
                    'optional': 'my_value1',
                },
            ],
            filter_audit={'key': 'my_key1'}
        )

    def test_audit_filter_on_audit_model_is_returning_only_selected_data(self):
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerAuditTest._controller.put({
            'key': 'my_key1',
            'mandatory': 2,
        })
        CRUDControllerAuditTest._controller.delete({'key': 'my_key1'})
        self._check_audit(
            [
                {
                    'audit_action': 'U',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 2,
                    'optional': 'my_value1',
                },
            ],
            filter_audit={'audit_action': 'U'}
        )

    def test_delete_without_filter_is_removing_everything(self):
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key1',
            'mandatory': 1,
            'optional': 'my_value1',
        })
        CRUDControllerAuditTest._controller.post({
            'key': 'my_key2',
            'mandatory': 2,
            'optional': 'my_value2',
        })
        self.assertEqual(2, CRUDControllerAuditTest._controller.delete({}))
        self.assertEqual([], CRUDControllerAuditTest._controller.get({}))
        self._check_audit(
            [
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'I',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key2',
                    'mandatory': 2,
                    'optional': 'my_value2',
                },
                {
                    'audit_action': 'D',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
                    'audit_user': '',
                    'key': 'my_key1',
                    'mandatory': 1,
                    'optional': 'my_value1',
                },
                {
                    'audit_action': 'D',
                    'audit_date_utc': '\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d.\d\d\d\d\d\d\+00\:00',
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
                'limit': int,
                'offset': int,
            },
            {arg.name: arg.type for arg in CRUDControllerAuditTest._controller.query_get_parser.args})
        self._check_audit([])

    def test_query_get_audit_parser(self):
        self.assertEqual(
            {
                'audit_action': str,
                'audit_date_utc': datetime.datetime,
                'audit_user': str,
                'key': str,
                'mandatory': int,
                'optional': str,
                'limit': int,
                'offset': int,
            },
            {arg.name: arg.type for arg in CRUDControllerAuditTest._controller.query_get_audit_parser.args})
        self._check_audit([])

    def test_query_delete_parser(self):
        self.assertEqual(
            {
                'key': str,
                'mandatory': int,
                'optional': str,
            },
            {arg.name: arg.type for arg in CRUDControllerAuditTest._controller.query_delete_parser.args})
        self._check_audit([])

    def test_get_response_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name in fields.keys()]
                test_fields.sort()
                return name, test_fields

        CRUDControllerAuditTest._controller.namespace(TestAPI)
        self.assertEqual(
            ('TestModel', ['key', 'mandatory', 'optional']),
            CRUDControllerAuditTest._controller.get_response_model)
        self._check_audit([])

    def test_get_audit_response_model(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name in fields.keys()]
                test_fields.sort()
                return name, test_fields

        CRUDControllerAuditTest._controller.namespace(TestAPI)
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
            CRUDControllerAuditTest._controller.get_audit_response_model)
        self._check_audit([])


class FlaskRestPlusModelsTest(unittest.TestCase):
    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_rest_plus_type_for_string_field_is_string(self):
        field = marshmallow_fields.String()
        self.assertEqual(flask_rest_plus_fields.String, flask_restplus_models.get_rest_plus_type(field))

    def test_rest_plus_type_for_int_field_is_integer(self):
        field = marshmallow_fields.Integer()
        self.assertEqual(flask_rest_plus_fields.Integer, flask_restplus_models.get_rest_plus_type(field))

    def test_rest_plus_type_for_bool_field_is_boolean(self):
        field = marshmallow_fields.Boolean()
        self.assertEqual(flask_rest_plus_fields.Boolean, flask_restplus_models.get_rest_plus_type(field))

    def test_rest_plus_type_for_date_field_is_date(self):
        field = marshmallow_fields.Date()
        self.assertEqual(flask_rest_plus_fields.Date, flask_restplus_models.get_rest_plus_type(field))

    def test_rest_plus_type_for_datetime_field_is_datetime(self):
        field = marshmallow_fields.DateTime()
        self.assertEqual(flask_rest_plus_fields.DateTime, flask_restplus_models.get_rest_plus_type(field))

    def test_rest_plus_type_for_decimal_field_is_fixed(self):
        field = marshmallow_fields.Decimal()
        self.assertEqual(flask_rest_plus_fields.Fixed, flask_restplus_models.get_rest_plus_type(field))

    def test_rest_plus_type_for_float_field_is_float(self):
        field = marshmallow_fields.Float()
        self.assertEqual(flask_rest_plus_fields.Float, flask_restplus_models.get_rest_plus_type(field))

    def test_rest_plus_type_for_number_field_is_decimal(self):
        field = marshmallow_fields.Number()
        self.assertEqual(flask_rest_plus_fields.Decimal, flask_restplus_models.get_rest_plus_type(field))

    def test_rest_plus_type_for_time_field_is_datetime(self):
        field = marshmallow_fields.Time()
        self.assertEqual(flask_rest_plus_fields.DateTime, flask_restplus_models.get_rest_plus_type(field))

    def test_rest_plus_type_for_field_field_is_string(self):
        field = marshmallow_fields.Field()
        self.assertEqual(flask_rest_plus_fields.String, flask_restplus_models.get_rest_plus_type(field))

    def test_rest_plus_type_for_none_field_cannot_be_guessed(self):
        with self.assertRaises(Exception) as cm:
            flask_restplus_models.get_rest_plus_type(None)
        self.assertEqual('Flask RestPlus field type cannot be guessed for None field.', cm.exception.args[0])

    def test_rest_plus_example_for_string_field(self):
        field = marshmallow_fields.String()
        self.assertEqual('sample_value', flask_restplus_models.get_example(field))

    def test_rest_plus_example_for_int_field_is_integer(self):
        field = marshmallow_fields.Integer()
        self.assertEqual('0', flask_restplus_models.get_example(field))

    def test_rest_plus_example_for_bool_field_is_true(self):
        field = marshmallow_fields.Boolean()
        self.assertEqual('true', flask_restplus_models.get_example(field))

    def test_rest_plus_example_for_date_field_is_YYYY_MM_DD(self):
        field = marshmallow_fields.Date()
        self.assertEqual('2017-09-24', flask_restplus_models.get_example(field))

    def test_rest_plus_example_for_datetime_field_is_YYYY_MM_DDTHH_MM_SS(self):
        field = marshmallow_fields.DateTime()
        self.assertEqual('2017-09-24T15:36:09', flask_restplus_models.get_example(field))

    def test_rest_plus_example_for_decimal_field_is_decimal(self):
        field = marshmallow_fields.Decimal()
        self.assertEqual('0.0', flask_restplus_models.get_example(field))

    def test_rest_plus_example_for_float_field_is_float(self):
        field = marshmallow_fields.Float()
        self.assertEqual('0.0', flask_restplus_models.get_example(field))

    def test_rest_plus_example_for_number_field_is_decimal(self):
        field = marshmallow_fields.Number()
        self.assertEqual('0.0', flask_restplus_models.get_example(field))

    def test_rest_plus_example_for_time_field_is_HH_MM_SS(self):
        field = marshmallow_fields.Time()
        self.assertEqual('15:36:09', flask_restplus_models.get_example(field))

    def test_rest_plus_example_for_none_field_is_sample_value(self):
        self.assertEqual('sample_value', flask_restplus_models.get_example(None))


class SQlAlchemyColumnsTest(unittest.TestCase):
    _model = None

    @classmethod
    def setUpClass(cls):
        database.load('sqlite:///:memory:', cls._create_models)

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            string_column = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            integer_column = sqlalchemy.Column(sqlalchemy.Integer)
            boolean_column = sqlalchemy.Column(sqlalchemy.Boolean)
            date_column = sqlalchemy.Column(sqlalchemy.Date)
            datetime_column = sqlalchemy.Column(sqlalchemy.DateTime)
            time_column = sqlalchemy.Column(sqlalchemy.Time)
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

    def test_python_type_for_sqlalchemy_string_field_is_string(self):
        field = self._model.schema().fields['string_column']
        self.assertEqual(str, flask_restplus_models.get_python_type(field))

    def test_python_type_for_sqlalchemy_integer_field_is_integer(self):
        field = self._model.schema().fields['integer_column']
        self.assertEqual(int, flask_restplus_models.get_python_type(field))

    def test_python_type_for_sqlalchemy_boolean_field_is_boolean(self):
        field = self._model.schema().fields['boolean_column']
        self.assertEqual(bool, flask_restplus_models.get_python_type(field))

    def test_python_type_for_sqlalchemy_date_field_is_date(self):
        field = self._model.schema().fields['date_column']
        self.assertEqual(datetime.date, flask_restplus_models.get_python_type(field))

    def test_python_type_for_sqlalchemy_datetime_field_is_datetime(self):
        field = self._model.schema().fields['datetime_column']
        self.assertEqual(datetime.datetime, flask_restplus_models.get_python_type(field))

    def test_python_type_for_sqlalchemy_time_field_is_time(self):
        field = self._model.schema().fields['time_column']
        self.assertEqual(datetime.time, flask_restplus_models.get_python_type(field))

    def test_python_type_for_sqlalchemy_float_field_is_float(self):
        field = self._model.schema().fields['float_column']
        self.assertEqual(float, flask_restplus_models.get_python_type(field))

    def test_python_type_for_sqlalchemy_none_field_cannot_be_guessed(self):
        with self.assertRaises(Exception) as cm:
            flask_restplus_models.get_python_type(None)
        self.assertEqual('Python field type cannot be guessed for None field.', cm.exception.args[0])


if __name__ == '__main__':
    unittest.main()
