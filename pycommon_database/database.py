import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, exc
from marshmallow_sqlalchemy import ModelSchema
import urllib.parse

from pycommon_database.flask_restplus_models import all_schema_fields, model_description, all_model_fields
from pycommon_database.flask_restplus_errors import ValidationFailed, ModelCouldNotBeFound, ModelNotProvided, MoreThanOneResult

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
        try:
            model = query.one_or_none()
        except exc.MultipleResultsFound:
            cls._session.rollback()  # SQLAlchemy state is not coherent with the reality if not rollback
            raise MoreThanOneResult(kwargs)
        return cls.schema().dump(model).data

    @classmethod
    def add(cls, model_as_dict: dict):
        """
        Add a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        """
        if not model_as_dict:
            raise ModelNotProvided()
        model, errors = cls.schema().load(model_as_dict, session=cls._session)
        if errors:
            raise ValidationFailed(model_as_dict, errors)
        try:
            cls._session.add(model)
            cls._session.commit()
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def update(cls, model_as_dict: dict):
        """
        Update a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        """
        if not model_as_dict:
            raise ModelNotProvided()
        previous_model = cls.schema().get_instance(model_as_dict)
        if not previous_model:
            raise ModelCouldNotBeFound(model_as_dict)
        for key, value in model_as_dict.items():
            setattr(previous_model, key, value)
        new_model_as_dict = inspect(previous_model).dict
        errors = cls.schema().validate(new_model_as_dict, session=cls._session)
        if errors:
            raise ValidationFailed(model_as_dict, errors)
        try:
            cls._session.merge(previous_model)
            cls._session.commit()
        except Exception as e:
            cls._session.rollback()
            raise

    @classmethod
    def remove(cls, **kwargs):
        """
        Remove the model(s) matching those criterion.
        :returns Number of removed rows.
        """
        try:
            query = cls._session.query(cls)
            for key, value in kwargs.items():
                if value is not None:
                    query = query.filter(getattr(cls, key) == value)
            nb_removed = query.delete()
            cls._session.commit()
            return nb_removed
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
        return Schema(session=cls._session)


class CRUDController:
    """
    Class providing methods to interact with a CRUDModel.
    """
    _model = None
    all_attributes = None

    @classmethod
    def model(cls, value):
        cls._model = value
        cls.all_attributes = all_model_fields(cls._model)

    def get(self, request_arguments):
        return self._model.get_all(**request_arguments)

    @classmethod
    def response_for_get(cls, api):
        return all_schema_fields(cls._model, api)

    def post(self, new_sample_dictionary):
        self._model.add(new_sample_dictionary)

    def put(self, updated_sample_dictionary):
        self._model.update(updated_sample_dictionary)

    def delete(self, request_arguments):
        return self._model.remove(**request_arguments)


def _retrieve_model_dictionary(sql_alchemy_class):
    description = {
        'table': sql_alchemy_class.__tablename__,
    }

    if hasattr(sql_alchemy_class, 'table_args__'):
        description['schema'] = sql_alchemy_class.table_args__.get('schema')

    mapper = inspect(sql_alchemy_class)
    for column in mapper.attrs:
        description[column.key] = column.columns[0].name

    return description


class ModelDescriptionController:
    _model = None
    _model_dictionary = None

    @classmethod
    def model(cls, value):
        cls._model = value
        cls._model_dictionary = _retrieve_model_dictionary(cls._model)

    def get(self):
        return self._model_dictionary

    @classmethod
    def response_for_get(cls, api):
        return model_description(cls._model, api)


class NoDatabaseProvided(Exception):
    def __init__(self):
        Exception.__init__(self, 'A database connection URL must be provided.')


class NoRelatedModels(Exception):
    def __init__(self):
        Exception.__init__(self, 'A method allowing to create related models must be provided.')


def load_from(database_connection_url: str, create_models_func, create_if_needed=True):
    """
    Create all necessary tables and perform the link between models and underlying database connection.

    :param database_connection_url: URL formatted as a standard database connection string (Mandatory).
    :param create_models_func: Function that will be called to create models and return them (instances of CRUDModel) (Mandatory).
    :param create_if_needed: Try to create tables if not found.
    """
    if not database_connection_url:
        raise NoDatabaseProvided()
    if not create_models_func:
        raise NoRelatedModels()
    logger.info(f'Connecting to {database_connection_url}...')
    logger.debug(f'Creating engine...')
    engine = create_engine(database_connection_url)
    logger.debug(f'Creating base...')
    base = declarative_base(bind=engine)
    logger.debug(f'Creating models...')
    model_classes = create_models_func(base)
    if create_if_needed:
        logger.debug(f'Creating tables...')
        base.metadata.create_all(bind=engine)
    logger.debug(f'Creating session...')
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
        logger.info(f'Resetting all data related to {base.metadata.bind.url}...')
        base.metadata.drop_all(bind=base.metadata.bind)
        base.metadata.create_all(bind=base.metadata.bind)
        logger.info(f'All data related to {base.metadata.bind.url} reset.')


def sybase_url(connection_parameters: str):
    return f'sybase+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_parameters)}'
