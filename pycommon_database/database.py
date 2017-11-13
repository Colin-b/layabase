import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.pool import StaticPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, exc
from marshmallow_sqlalchemy import ModelSchema
import urllib.parse
from flask_restplus import inputs

from pycommon_database.flask_restplus_models import (
    model_with_fields,
    model_describing_sql_alchemy_mapping,
    query_parser_with_fields,
)
from pycommon_database.flask_restplus_errors import ValidationFailed, ModelCouldNotBeFound
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
        try:
            all_models = query.all()
            return cls.schema().dump(all_models, many=True).data
        except exc.sa_exc.DBAPIError:
            logger.exception('Database could not be reached.')
            raise Exception('Database could not be reached.')


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
            raise ValidationFailed(kwargs, message='More than one result: Consider another filtering.')
        except exc.sa_exc.DBAPIError:
            logger.exception('Database could not be reached.')
            raise Exception('Database could not be reached.')
        return cls.schema().dump(model).data

    @classmethod
    def add_all(cls, models_as_list_of_dict: list):
        """
        Add models formatted as a list of dictionaries.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted model formatted as a list of dictionaries.
        """
        if not models_as_list_of_dict:
            raise ValidationFailed({}, message='No data provided.')
        try:
            models, errors = cls.schema().load(models_as_list_of_dict, many=True, session=cls._session)
        except exc.sa_exc.DBAPIError:
            logger.exception('Database could not be reached.')
            raise Exception('Database could not be reached.')
        if errors:
            raise ValidationFailed(models_as_list_of_dict, marshmallow_errors=errors)
        try:
            cls._session.add_all(models)
            ret = cls._session.commit()
            return _models_field_values(models)
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            logger.exception('Database could not be reached.')
            raise Exception('Database could not be reached.')
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def add(cls, model_as_dict: dict):
        """
        Add a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted model formatted as a dictionary.
        """
        if not model_as_dict:
            raise ValidationFailed({}, message='No data provided.')
        try:
            model, errors = cls.schema().load(model_as_dict, session=cls._session)
        except exc.sa_exc.DBAPIError:
            logger.exception('Database could not be reached.')
            raise Exception('Database could not be reached.')
        if errors:
            raise ValidationFailed(model_as_dict, marshmallow_errors=errors)
        try:
            cls._session.add(model)
            ret = cls._session.commit()
            return _model_field_values(model)
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            logger.exception('Database could not be reached.')
            raise Exception('Database could not be reached.')
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
            raise ValidationFailed({}, message='No data provided.')
        try:
            previous_model = cls.schema().get_instance(model_as_dict)
        except exc.sa_exc.DBAPIError:
            logger.exception('Database could not be reached.')
            raise Exception('Database could not be reached.')
        if not previous_model:
            raise ModelCouldNotBeFound(model_as_dict)
        previous_model_as_dict = _model_field_values(previous_model)
        new_model, errors = cls.schema().load(model_as_dict, instance=previous_model, partial=True, session=cls._session)
        if errors:
            raise ValidationFailed(model_as_dict, marshmallow_errors=errors)
        new_model_as_dict = _model_field_values(new_model)
        try:
            cls._session.add(new_model)
            cls._session.commit()
            return previous_model_as_dict, new_model_as_dict
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            logger.exception('Database could not be reached.')
            raise Exception('Database could not be reached.')
        except Exception:
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
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            logger.exception('Database could not be reached.')
            raise Exception('Database could not be reached.')
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

        schema = Schema(session=cls._session)
        mapper = inspect(cls)
        for attr in mapper.attrs:
            schema_field = schema.fields.get(attr.key, None)
            if schema_field:
                cls.enrich_schema_field(schema_field, attr)

        return schema

    @classmethod
    def post_schema(cls):
        """
        Create a new Marshmallow SQL Alchemy schema instance.

        :return: The newly created schema instance.
        """
        class PostSchema(ModelSchema):
            class Meta:
                model = cls

        post_schema = PostSchema(session=cls._session)
        mapper = inspect(cls)
        for attr in mapper.attrs:
            schema_field = post_schema.fields.get(attr.key, None)
            if schema_field:
                cls.enrich_schema_field(schema_field, attr)
                cls.enrich_post_schema_field(schema_field, attr)

        return post_schema

    @classmethod
    def enrich_schema_field(cls, marshmallow_field, sql_alchemy_field):
        defaults = [column.default.arg for column in sql_alchemy_field.columns if column.default]
        if defaults:
            marshmallow_field.metadata['sqlalchemy_default'] = defaults[0]

    @classmethod
    def enrich_post_schema_field(cls, marshmallow_field, sql_alchemy_field):
        auto_increments = [column.autoincrement for column in sql_alchemy_field.columns]
        marshmallow_field.dump_only = True in auto_increments


class ControllerModelNotSet(Exception):
    def __init__(self, controller_class):
        Exception.__init__(self,
                           f'Model was not attached to {controller_class.__name__}. Call {controller_class.model}.')


class CRUDController:
    """
    Class providing methods to interact with a CRUDModel.
    """
    _model = None
    _marshmallow_fields = None
    _audit_model = None
    _audit_marshmallow_fields = None

    # CRUD request parsers
    query_get_parser = None
    query_delete_parser = None
    query_get_audit_parser = None

    # CRUD model definition (instead of request parsers)
    json_post_model = None
    json_put_model = None

    # CRUD response marshallers
    get_response_model = None
    get_audit_response_model = None
    get_model_description_response_model = None

    # The response that is always sent for the Model Description
    _model_description_dictionary = None

    @classmethod
    def model(cls, value, audit: bool=False):
        """
        Initialize related model (should extends CRUDModel).

        :param value: CRUDModel and SQLAlchemy model.
        :param audit: True to add an extra model representing the audit table. No audit by default.
        """
        cls._model = value
        cls._marshmallow_fields = cls._model.schema().fields.values()

        cls.query_get_parser = query_parser_with_fields(cls._marshmallow_fields)
        cls.query_get_parser.add_argument('limit', type=inputs.positive)
        if _supports_offset(cls._model.metadata.bind.url.drivername):
            cls.query_get_parser.add_argument('offset', type=inputs.natural)
        cls.query_delete_parser = query_parser_with_fields(cls._marshmallow_fields)
        if audit:
            cls._audit_model = create_audit_model(cls._model)
            cls._audit_marshmallow_fields = cls._audit_model.schema().fields.values()
            cls.query_get_audit_parser = query_parser_with_fields(cls._audit_marshmallow_fields)
            cls.query_get_audit_parser.add_argument('limit', type=inputs.positive)
            if _supports_offset(cls._model.metadata.bind.url.drivername):
                cls.query_get_audit_parser.add_argument('offset', type=inputs.natural)
        else:
            cls._audit_model = None
            cls._audit_marshmallow_fields = None
            cls.query_get_audit_parser = None
        cls._model_description_dictionary = _retrieve_model_description_dictionary(cls._model)

    @classmethod
    def namespace(cls, namespace):
        """
        Create Flask RestPlus models that can be used to marshall results (and document service).
        This method should always be called AFTER cls.model()

        :param namespace: Flask RestPlus API.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        post_marshmallow_fields = [field for field in cls._model.post_schema().fields.values() if not field.dump_only]
        update_model_is_insert_model = len(post_marshmallow_fields) == len(cls._marshmallow_fields)
        insert_model_name_suffix = '' if update_model_is_insert_model else '_Insert'
        cls.json_post_model = model_with_fields(namespace, cls._model.__name__ + insert_model_name_suffix, post_marshmallow_fields)
        cls.json_put_model = model_with_fields(namespace, cls._model.__name__, cls._marshmallow_fields)
        cls.get_response_model = model_with_fields(namespace, cls._model.__name__, cls._marshmallow_fields)
        if cls._audit_model:
            cls.get_audit_response_model = model_with_fields(namespace, 'Audit' + cls._model.__name__, cls._audit_marshmallow_fields)
        else:
            cls.get_audit_response_model = None
        cls.get_model_description_response_model = model_describing_sql_alchemy_mapping(namespace, cls._model)

    @classmethod
    def get(cls, request_arguments: dict):
        """
        Return all models formatted as a list of dictionaries.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        return cls._model.get_all(**request_arguments)

    @classmethod
    def post(cls, new_sample_dictionary: dict):
        """
        Add a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted model formatted as a dictionary.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        new_sample_model = cls._model.add(new_sample_dictionary)
        if cls._audit_model:
            cls._audit_model._session = cls._model._session
            cls._audit_model.audit_add(new_sample_model)
        return new_sample_model

    @classmethod
    def post_many(cls, new_sample_dictionaries_list: list):
        """
        Add models formatted as a list of dictionaries.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted models formatted as a list of dictionaries.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        new_sample_models = cls._model.add_all(new_sample_dictionaries_list)
        if cls._audit_model:
            cls._audit_model._session = cls._model._session
            for new_sample_model in new_sample_models:
                cls._audit_model.audit_add(new_sample_model)
        return new_sample_models

    @classmethod
    def put(cls, updated_sample_dictionary: dict):
        """
        Update a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns A tuple containing previous model formatted as a dictionary (first item)
        and new model formatted as a dictionary (second item).
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        updated_sample_model = cls._model.update(updated_sample_dictionary)
        if cls._audit_model:
            cls._audit_model._session = cls._model._session
            cls._audit_model.audit_update(updated_sample_model[1])
        return updated_sample_model

    @classmethod
    def delete(cls, request_arguments: dict):
        """
        Remove the model(s) matching those criterion.
        :returns Number of removed rows.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        if cls._audit_model:
            cls._audit_model._session = cls._model._session
            cls._audit_model.audit_remove(**request_arguments)
        return cls._model.remove(**request_arguments)

    @classmethod
    def get_audit(cls, request_arguments: dict):
        """
        Return all audit models formatted as a list of dictionaries.
        """
        if not cls._audit_model:
            return []
        cls._audit_model._session = cls._model._session
        return cls._audit_model.get_all(**request_arguments)

    @classmethod
    def get_model_description(cls):
        if not cls._model_description_dictionary:
            raise ControllerModelNotSet(cls)
        return cls._model_description_dictionary


def _retrieve_model_description_dictionary(sql_alchemy_class):
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


def _models_field_values(model_instances: list):
        """Return models fields values (with the proper type) as a list of dictionaries."""
        if not model_instances:
            return []
        return model_instances[0].schema().dump(model_instances, many=True).data


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


def _supports_offset(driver_name: str):
    return not driver_name.startswith('sybase')


def _in_memory(database_connection_url: str):
    return ':memory:' in database_connection_url


def _prepare_engine(engine):
    if engine.url.drivername.startswith('sybase'):
        engine.dialect.identifier_preparer.initial_quote = '['
        engine.dialect.identifier_preparer.final_quote = ']'


def _get_view_names(engine, schema) -> list:
    with engine.connect() as conn:
        return engine.dialect.get_view_names(conn, schema)


def load(database_connection_url: str, create_models_func: callable):
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
    if _in_memory(database_connection_url):
        engine = create_engine(database_connection_url, poolclass=StaticPool, connect_args={'check_same_thread': False})
    else:
        engine = create_engine(database_connection_url)
    _prepare_engine(engine)
    logger.debug(f'Creating base...')
    base = declarative_base(bind=engine)
    logger.debug(f'Creating models...')
    model_classes = create_models_func(base)
    if _can_retrieve_metadata(database_connection_url):
        all_view_names = _get_view_names(engine, base.metadata.schema)
        all_tables_and_views = base.metadata.tables
        # Remove all views from table list before creating them
        base.metadata.tables = {
            table_name: table_or_view
            for table_name, table_or_view in all_tables_and_views.items()
            if table_name not in all_view_names
        }
        logger.debug(f'Creating tables...')
        base.metadata.create_all(bind=engine)
        base.metadata.tables = all_tables_and_views
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
