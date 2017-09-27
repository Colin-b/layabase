import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from marshmallow_sqlalchemy import ModelSchema
import urllib.parse

from pycommon_database.flask_restplus_models import all_schema_fields

logger = logging.getLogger(__name__)


class CRUDModel:
    """
    Class providing CRUD helper methods for a SQL Alchemy model.
    _session class property must be specified in Model.
    Calling load_from(...) will provide you one.
    """
    _session = None

    @classmethod
    def get_all(cls, **kwargs):
        """
        Return all models formatted as a list of dictionaries.
        """
        query = cls._session.query(cls)
        for key, value in kwargs.items():
            if value is not None:
                query = query.filter(getattr(cls, key) == value)
        all_models = query.all()
        return cls.schema().dump(all_models, many=True).data

    @classmethod
    def get(cls, **kwargs):
        """
        Return the model formatted as a dictionary.
        """
        query = cls._session.query(cls)
        for key, value in kwargs.items():
            if value is not None:
                query = query.filter(getattr(cls, key) == value)
        model = query.one_or_none()
        return cls.schema().dump(model).data

    @classmethod
    def add(cls, model_as_dict: dict):
        """
        Add a model formatted as a dictionary.
        """
        model = cls.schema().load(model_as_dict, session=cls._session).data
        try:
            cls._session.add(model)
            cls._session.commit()
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def remove(cls, **kwargs):
        """
        Remove the model(s) matching those criterion.
        """
        try:
            query = cls._session.query(cls)
            for key, value in kwargs.items():
                if value is not None:
                    query = query.filter(getattr(cls, key) == value)
            query.delete()
            cls._session.commit()
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def schema(cls):
        """
        Create a new Marshmallow SQL Alchemy schema instance.

        :return: The newly created schema instance.
        """
        class Schema(ModelSchema):
            class Meta:
                model = cls
        return Schema()


class CRUDController:
    """
    Class providing methods to interact with a CRUDModel.
    """
    _model = None

    def get(self, request_arguments):
        return self._model.get_all(**request_arguments)

    @classmethod
    def response_for_get(cls, api):
        return all_schema_fields(cls._model, api)

    def post(self, new_sample_dictionary):
        self._model.add(new_sample_dictionary)

    def put(self, updated_sample_dictionary):
        self._model.add(updated_sample_dictionary)

    def delete(self, request_arguments):
        self._model.remove(**request_arguments)


def load_from(database_connection_url: str, create_models_func, create_if_needed=True):
    """
    Create all necessary tables and perform the link between models and underlying database connection.

    :param database_connection_url: URL formatted as a standard database connection string.
    :param create_models_func: Function that will be called to create models and return them (instances of CRUDModel).
    :param create_if_needed: Try to create tables if not found.
    """
    logger.info(f'Connecting to {database_connection_url}...')
    engine = create_engine(database_connection_url)
    base = declarative_base(bind=engine)
    model_classes = create_models_func(base)
    if create_if_needed:
        base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    logger.info(f'Connected to {database_connection_url}.')
    for model_class in model_classes:
        model_class._session = session
    return base


def reset(base):
    """
    If the database was already created, then drop all tables and recreate them all.
    """
    if base:
        logger.info(f'Reset all data related to {base.metadata.bind.url}.')
        base.metadata.drop_all()
        base.metadata.create_all()


def sybase_url(connection_parameters: str):
    return f'sybase+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_parameters)}'
