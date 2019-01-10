import unittest
import sqlalchemy
import logging
import sys

logging.basicConfig(
    format='%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.DEBUG)

from pycommon_database import database, database_sqlalchemy
from pycommon_test import mock_now

logger = logging.getLogger(__name__)


class SQLAlchemyCRUDModelTest(unittest.TestCase):
    _db = None
    _model = None

    @classmethod
    def setUpClass(cls):
        mock_now()
        cls._db = database.load('sqlite:///:memory:', cls._create_models)
        cls._db.metadata.bind.dispose()
        cls._db.metadata.bind.engine.execute = lambda x: exec('raise(Exception(x))')

    @classmethod
    def _create_models(cls, base):
        logger.info('Declare model class...')

        class TestModel(database_sqlalchemy.CRUDModel, base):
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
            self._model.get_all()
        self.assertEqual('Database could not be reached.', cm.exception.args[0])

    def test_get_when_db_down(self):
        with self.assertRaises(Exception) as cm:
            self._model.get()
        self.assertEqual('Database could not be reached.', cm.exception.args[0])

    def test_add_when_db_down(self):
        with self.assertRaises(Exception) as cm:
            self._model.add({
                'key': 'my_key1',
                'mandatory': 1,
                'optional': 'my_value1',
            })
        self.assertEqual('Database could not be reached.', cm.exception.args[0])

    def test_update_when_db_down(self):
        with self.assertRaises(Exception) as cm:
            self._model.update({
                'key': 'my_key1',
                'mandatory': 1,
                'optional': 'my_value1',
            })
        self.assertEqual('Database could not be reached.', cm.exception.args[0])

    def test_remove_when_db_down(self):
        with self.assertRaises(Exception) as cm:
            self._model.remove()
        self.assertEqual('Database could not be reached.', cm.exception.args[0])

    def test_health_details_failure(self):
        self.assertEqual((
            'fail',
            {
                'sqlite:select': {
                    'componentType': 'datastore',
                    'status': 'fail',
                    'time': '2018-10-11T15:05:05.663979',
                    'output': 'SELECT 1'
                }
            }
        ), database.health_details(self._db))

if __name__ == '__main__':
    unittest.main()
