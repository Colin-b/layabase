import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, exc
from marshmallow_sqlalchemy import ModelSchema
import urllib.parse

from pycommon_database.flask_restplus_models import all_schema_fields, model_description, all_model_fields, all_schema_fields_as_json
from pycommon_database.flask_restplus_errors import ValidationFailed, ModelCouldNotBeFound, ModelNotProvided, MoreThanOneResult
from pycommon_database.audit import create_from as create_audit_model

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
            if key == 'limit':
                query = query.limit(value)
            elif key == 'offset':
                query = query.offset(value)
            elif value is not None:
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
        :returns The inserted model formatted as a dictionary.
        """
        if not model_as_dict:
            raise ModelNotProvided()
        model, errors = cls.schema().load(model_as_dict, session=cls._session)
        if errors:
            raise ValidationFailed(model_as_dict, errors)
        try:
            cls._session.add(model)
            ret = cls._session.commit()
            return _model_field_values(model)
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def update(cls, model_as_dict: dict):
        """
        Update a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns A tuple containing previous model formatted as a dictionary (first item)
        and new model formatted as a dictionary (second item).
        """
        if not model_as_dict:
            raise ModelNotProvided()
        previous_model = cls.schema().get_instance(model_as_dict)
        if not previous_model:
            raise ModelCouldNotBeFound(model_as_dict)
        previous_model_as_dict = _model_field_values(previous_model)
        for key, value in model_as_dict.items():
            setattr(previous_model, key, value)
        new_model_as_dict = _model_field_values(previous_model)
        errors = cls.schema().validate(inspect(previous_model).dict, session=cls._session)
        if errors:
            raise ValidationFailed(model_as_dict, errors)
        try:
            cls._session.merge(previous_model)
            cls._session.commit()
            return previous_model_as_dict, new_model_as_dict
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
    _audit_model = None
    all_attributes = None
    all_attributes_as_json = None

    @classmethod
    def model(cls, value, audit=False):
        """
        Initialize related model (should extends CRUDModel).

        :param value: CRUDModel
        :param audit: True to add an extra model representing the audit table. No audit by default.
        """
        cls._model = value
        cls.all_attributes = all_model_fields(cls._model)
        cls.all_attributes.add_argument('limit', type=int)
        cls.all_attributes.add_argument('offset', type=int)
        cls._audit_model = create_audit_model(cls._model) if audit else None

    def get(self, request_arguments):
        """
        Return all models formatted as a list of dictionaries.
        """
        return self._model.get_all(**request_arguments)

    @classmethod
    def namespace(cls, namespace):
        cls.all_attributes_as_json = all_schema_fields_as_json(cls._model, namespace)

    @classmethod
    def response_for_get(cls, api):
        return all_schema_fields(cls._model, api)

    def post(self, new_sample_dictionary):
        """
        Add a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted model formatted as a dictionary.
        """
        if self._audit_model:
            self._audit_model._session = self._model._session
            self._audit_model.audit_add(new_sample_dictionary)
        return self._model.add(new_sample_dictionary)

    def put(self, updated_sample_dictionary):
        """
        Update a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns A tuple containing previous model formatted as a dictionary (first item)
        and new model formatted as a dictionary (second item).
        """
        if self._audit_model:
            self._audit_model._session = self._model._session
            self._audit_model.audit_update(updated_sample_dictionary)
        return self._model.update(updated_sample_dictionary)

    def delete(self, request_arguments):
        """
        Remove the model(s) matching those criterion.
        :returns Number of removed rows.
        """
        if self._audit_model:
            self._audit_model._session = self._model._session
            self._audit_model.audit_remove(**request_arguments)
        return self._model.remove(**request_arguments)

    def get_audit(self, request_arguments):
        if not self._audit_model:
            return []
        self._audit_model._session = self._model._session
        return self._audit_model.get_all(**request_arguments)


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


def _model_field_values(model_instance):
    """Return model fields values (with the proper type) as a dictionary."""
    return model_instance.schema().dump(model_instance).data


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


def _clean_database_url(database_connection_url: str):
    connection_details = database_connection_url.split(':///?odbc_connect=', maxsplit=1)
    if len(connection_details) == 2:
        return f'{connection_details[0]}:///?odbc_connect={urllib.parse.quote_plus(connection_details[1])}'
    return database_connection_url


def _can_retrieve_metadata(database_connection_url: str):
    return not database_connection_url.startswith('sybase')


def load(database_connection_url: str, create_models_func):
    """
    Create all necessary tables and perform the link between models and underlying database connection.

    :param database_connection_url: URL formatted as a standard database connection string (Mandatory).
    :param create_models_func: Function that will be called to create models and return them (instances of CRUDModel) (Mandatory).
    """
    if not database_connection_url:
        raise NoDatabaseProvided()
    if not create_models_func:
        raise NoRelatedModels()
    database_connection_url = _clean_database_url(database_connection_url)
    logger.info(f'Connecting to {database_connection_url}...')
    logger.debug(f'Creating engine...')
    engine = create_engine(database_connection_url)
    logger.debug(f'Creating base...')
    base = declarative_base(bind=engine)
    logger.debug(f'Creating models...')
    model_classes = create_models_func(base)
    if _can_retrieve_metadata(database_connection_url):
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
