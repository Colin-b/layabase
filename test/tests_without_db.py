import unittest
import sqlalchemy
import logging
import sys
import datetime
from marshmallow_sqlalchemy.fields import fields as marshmallow_fields
from flask_restplus import fields as flask_rest_plus_fields, inputs

logging.basicConfig(
    format='%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.DEBUG)

from pycommon_database import database, flask_restplus_errors, flask_restplus_models

logger = logging.getLogger(__name__)


class CRUDModelTest(unittest.TestCase):
    _db = None
    _model = None

    @classmethod
    def setUpClass(cls):
        cls._db = database.load('sqlite:///:memory:', cls._create_models)
        cls._db.metadata.bind.dispose()

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

    def tearDown(self):
        logger.info(f'End of {self._testMethodName}')
        logger.info(f'-------------------------------')

    def test_get_all_when_db_down(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.get_all()
        self.assertEqual('Database could not be reached.', cm.exception.args[0])

    def test_get_when_db_down(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.get()
        self.assertEqual('Database could not be reached.', cm.exception.args[0])

    def test_add_when_db_down(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.add({
                'key': 'my_key1',
                'mandatory': 1,
                'optional': 'my_value1',
            })
        self.assertEqual('Database could not be reached.', cm.exception.args[0])

    def test_update_when_db_down(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.update({
                'key': 'my_key1',
                'mandatory': 1,
                'optional': 'my_value1',
            })
        self.assertEqual('Database could not be reached.', cm.exception.args[0])

    def test_remove_when_db_down(self):
        with self.assertRaises(Exception) as cm:
            CRUDModelTest._model.remove()
        self.assertEqual('Database could not be reached.', cm.exception.args[0])


if __name__ == '__main__':
    unittest.main()
