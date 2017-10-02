import unittest
import sqlalchemy
import logging
import sys

logging.basicConfig(
    format='%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.DEBUG)

from pycommon_database import database, flask_restplus_errors, flask_restplus_models

logger = logging.getLogger(__name__)


class DatabaseTest(unittest.TestCase):

    def test_none_connection_string_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            database.load_from(None, None)
        self.assertEqual('A database connection URL must be provided.', cm.exception.args[0])

    def test_empty_connection_string_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            database.load_from('', None)
        self.assertEqual('A database connection URL must be provided.', cm.exception.args[0])

    def test_no_create_models_function_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            database.load_from('sqlite:///:memory:', None)
        self.assertEqual('A method allowing to create related models must be provided.', cm.exception.args[0])

    def test_models_are_added_to_metadata(self):
        def create_models(base):
            class TestModel(base):
                __tablename__ = 'sample_table_name'

                key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)

            return [TestModel]

        db = database.load_from('sqlite:///:memory:', create_models)
        self.assertEqual('sqlite:///:memory:', str(db.metadata.bind.engine.url))
        self.assertEqual(['sample_table_name'], [table for table in db.metadata.tables.keys()])

    def test_sybase_url(self):
        self.assertEqual('sybase+pyodbc:///?odbc_connect=TEST%3DVALUE%3BTEST2%3DVALUE2',
                         database.sybase_url('TEST=VALUE;TEST2=VALUE2'))


class CRUDModelTest(unittest.TestCase):

    _db = None
    _model = None

    @classmethod
    def setUpClass(cls):
        cls._db = database.load_from('sqlite:///:memory:', cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.String, nullable=False)
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
                'mandatory': 'my_value',
            })
        self.assertEqual({'key': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'mandatory': 'my_value'}, cm.exception.received_data)

    def test_add_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.add({
                'key': 256,
                'mandatory': 'value1',
            })
        self.assertEqual({'key': ['Not a valid string.']}, cm.exception.errors)
        self.assertEqual({'key': 256, 'mandatory': 'value1'}, cm.exception.received_data)

    def test_update_with_wrong_type_is_invalid(self):
        CRUDModelTest._model.add({
            'key': 'value1',
            'mandatory': 'value1',
        })
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.update({
                'key': 'value1',
                'mandatory': 1,
            })
        self.assertEqual({'mandatory': ['Not a valid string.']}, cm.exception.errors)
        self.assertEqual({'key': 'value1', 'mandatory': 1}, cm.exception.received_data)

    def test_add_without_optional_is_valid(self):
        self.assertIsNone(CRUDModelTest._model.add({
            'key': 'my_key',
            'mandatory': 'my_value',
        }))

    def test_add_with_optional_is_valid(self):
        self.assertIsNone(CRUDModelTest._model.add({
            'key': 'my_key',
            'mandatory': 'my_value',
            'optional': 'my_value',
        }))

    def test_add_with_unknown_field_is_valid(self):
        self.assertIsNone(CRUDModelTest._model.add({
            'key': 'my_key',
            'mandatory': 'my_value',
            'optional': 'my_value',
            # This field do not exists in schema
            'unknown': 'my_value',
        }))

    def test_get_without_filter_is_retrieving_the_only_item(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        self.assertEqual(
            {
                'mandatory': 'my_value1',
                'optional': 'my_value1',
                'key': 'my_key1'
            },
            CRUDModelTest._model.get())

    def test_get_without_filter_is_failing_if_more_than_one_item_exists(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 'my_value2',
            'optional': 'my_value2',
        })
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.get()
        self.assertEqual({'': ['More than one result: Consider another filtering.']}, cm.exception.errors)
        self.assertEqual({}, cm.exception.received_data)

    def test_get_all_without_filter_is_retrieving_everything(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 'my_value2',
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 'my_value1', 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 'my_value2', 'optional': 'my_value2'}
            ],
            CRUDModelTest._model.get_all())

    def test_get_all_with_filter_is_retrieving_subset(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 'my_value2',
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 'my_value1', 'optional': 'my_value1'},
            ],
            CRUDModelTest._model.get_all(optional='my_value1'))

    def test_get_with_filter_is_retrieving_the_proper_row(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 'my_value2',
            'optional': 'my_value2',
        })
        self.assertEqual({'key': 'my_key1', 'mandatory': 'my_value1', 'optional': 'my_value1'},
                         CRUDModelTest._model.get(optional='my_value1'))

    def test_update_is_updating(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDModelTest._model.update({
            'key': 'my_key1',
            'optional': 'my_value',
        })
        self.assertEqual({'key': 'my_key1', 'mandatory': 'my_value1', 'optional': 'my_value'},
                         CRUDModelTest._model.get(mandatory='my_value1'))

    def test_update_is_updating_and_previous_value_cannot_be_used_to_filter(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 'my_value1',
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
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 'my_value2',
            'optional': 'my_value2',
        })
        self.assertEqual(1, CRUDModelTest._model.remove(key='my_key1'))
        self.assertEqual([{'key': 'my_key2', 'mandatory': 'my_value2', 'optional': 'my_value2'}],
                         CRUDModelTest._model.get_all())

    def test_remove_without_filter_is_removing_everything(self):
        CRUDModelTest._model.add({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDModelTest._model.add({
            'key': 'my_key2',
            'mandatory': 'my_value2',
            'optional': 'my_value2',
        })
        self.assertEqual(2, CRUDModelTest._model.remove())
        self.assertEqual([], CRUDModelTest._model.get_all())


class CRUDControllerTest(unittest.TestCase):

    class TestController(database.CRUDController):
        pass

    _db = None
    _controller = TestController()

    @classmethod
    def setUpClass(cls):
        cls._db = database.load_from('sqlite:///:memory:', cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.String, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        logger.info('Save model class...')
        cls._controller.model(TestModel)
        return [TestModel]

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
                'mandatory': 'my_value',
            })
        self.assertEqual({'key': ['Missing data for required field.']}, cm.exception.errors)
        self.assertEqual({'mandatory': 'my_value'}, cm.exception.received_data)

    def test_post_with_wrong_type_is_invalid(self):
        with self.assertRaises(Exception) as cm:
            CRUDControllerTest._controller.post({
                'key': 256,
                'mandatory': 'value1',
            })
        self.assertEqual({'key': ['Not a valid string.']}, cm.exception.errors)
        self.assertEqual({'key': 256, 'mandatory': 'value1'}, cm.exception.received_data)

    def test_put_with_wrong_type_is_invalid(self):
        CRUDControllerTest._controller.post({
            'key': 'value1',
            'mandatory': 'value1',
        })
        with self.assertRaises(Exception) as cm:
            CRUDControllerTest._controller.put({
                'key': 'value1',
                'mandatory': 1,
            })
        self.assertEqual({'mandatory': ['Not a valid string.']}, cm.exception.errors)
        self.assertEqual({'key': 'value1', 'mandatory': 1}, cm.exception.received_data)

    def test_post_without_optional_is_valid(self):
        self.assertIsNone(CRUDControllerTest._controller.post({
            'key': 'my_key',
            'mandatory': 'my_value',
        }))

    def test_post_with_optional_is_valid(self):
        self.assertIsNone(CRUDControllerTest._controller.post({
            'key': 'my_key',
            'mandatory': 'my_value',
            'optional': 'my_value',
        }))

    def test_post_with_unknown_field_is_valid(self):
        self.assertIsNone(CRUDControllerTest._controller.post({
            'key': 'my_key',
            'mandatory': 'my_value',
            'optional': 'my_value',
            # This field do not exists in schema
            'unknown': 'my_value',
        }))

    def test_get_without_filter_is_retrieving_the_only_item(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        self.assertEqual(
            [{
                'mandatory': 'my_value1',
                'optional': 'my_value1',
                'key': 'my_key1'
            }],
            CRUDControllerTest._controller.get({}))

    def test_get_without_filter_is_retrieving_everything(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key2',
            'mandatory': 'my_value2',
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 'my_value1', 'optional': 'my_value1'},
                {'key': 'my_key2', 'mandatory': 'my_value2', 'optional': 'my_value2'}
            ],
            CRUDControllerTest._controller.get({}))

    def test_get_with_filter_is_retrieving_subset(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key2',
            'mandatory': 'my_value2',
            'optional': 'my_value2',
        })
        self.assertEqual(
            [
                {'key': 'my_key1', 'mandatory': 'my_value1', 'optional': 'my_value1'},
            ],
            CRUDControllerTest._controller.get({'optional': 'my_value1'}))

    def test_put_is_updating(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.put({
            'key': 'my_key1',
            'optional': 'my_value',
        })
        self.assertEqual([{'key': 'my_key1', 'mandatory': 'my_value1', 'optional': 'my_value'}],
                         CRUDControllerTest._controller.get({'mandatory': 'my_value1'}))

    def test_put_is_updating_and_previous_value_cannot_be_used_to_filter(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 'my_value1',
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
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key2',
            'mandatory': 'my_value2',
            'optional': 'my_value2',
        })
        self.assertEqual(1, CRUDControllerTest._controller.delete({'key': 'my_key1'}))
        self.assertEqual([{'key': 'my_key2', 'mandatory': 'my_value2', 'optional': 'my_value2'}],
                         CRUDControllerTest._controller.get({}))

    def test_delete_without_filter_is_removing_everything(self):
        CRUDControllerTest._controller.post({
            'key': 'my_key1',
            'mandatory': 'my_value1',
            'optional': 'my_value1',
        })
        CRUDControllerTest._controller.post({
            'key': 'my_key2',
            'mandatory': 'my_value2',
            'optional': 'my_value2',
        })
        self.assertEqual(2, CRUDControllerTest._controller.delete({}))
        self.assertEqual([], CRUDControllerTest._controller.get({}))

    def test_all_attributes(self):
        self.assertEqual(
            {
                'key': str,
                'mandatory': str,
                'optional': str,
            },
            {arg.name: arg.type for arg in CRUDControllerTest._controller.all_attributes.args})

    def test_response_for_get(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name in fields.keys()]
                test_fields.sort()
                return name, test_fields
        self.assertEqual(
            ('TestModel', ['key', 'mandatory', 'optional']),
            CRUDControllerTest._controller.response_for_get(TestAPI))


class ModelDescriptionControllerTest(unittest.TestCase):

    class TestController(database.ModelDescriptionController):
        pass

    _db = None
    _controller = TestController()

    @classmethod
    def setUpClass(cls):
        cls._db = database.load_from('sqlite:///:memory:', cls._create_models)

    @classmethod
    def tearDownClass(cls):
        database.reset(cls._db)

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database.CRUDModel, base):
            __tablename__ = 'sample_table_name'

            key = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            mandatory = sqlalchemy.Column(sqlalchemy.String, nullable=False)
            optional = sqlalchemy.Column(sqlalchemy.String)

        logger.info('Save model class...')
        cls._controller.model(TestModel)
        return [TestModel]

    def setUp(self):
        logger.info(f'-------------------------------')
        logger.info(f'Start of {self._testMethodName}')

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_get_returns_description(self):
        self.assertEqual(
            {
                'key': 'key',
                'mandatory': 'mandatory',
                'optional': 'optional',
                'table': 'sample_table_name'
            },
            ModelDescriptionControllerTest._controller.get())

    def test_response_for_get(self):
        class TestAPI:
            @classmethod
            def model(cls, name, fields):
                test_fields = [name for name in fields.keys()]
                test_fields.sort()
                return name, test_fields
        self.assertEqual(
            ('TestModelDescription', ['key', 'mandatory', 'optional', 'table']),
            ModelDescriptionControllerTest._controller.response_for_get(TestAPI))


if __name__ == '__main__':
    unittest.main()
