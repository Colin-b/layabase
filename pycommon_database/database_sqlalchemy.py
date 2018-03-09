import logging
import urllib.parse
from sqlalchemy import create_engine, inspect
from sqlalchemy.pool import StaticPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, exc
from marshmallow import validate
from marshmallow_sqlalchemy import ModelSchema
from marshmallow_sqlalchemy.fields import fields as marshmallow_fields
from flask_restplus import fields as flask_restplus_fields, reqparse, inputs
from typing import List

from pycommon_database.flask_restplus_errors import ValidationFailed, ModelCouldNotBeFound

logger = logging.getLogger(__name__)


class CRUDModel:
    """
    Class providing CRUD helper methods for a SQL Alchemy model.
    _session class property must be specified in Model.
    Calling load_from(...) will provide you one.
    """
    _session = None
    audit_model = None

    @classmethod
    def _post_init(cls, session):
        cls._session = session
        if cls.audit_model:
            cls.audit_model._post_init(session)

    @classmethod
    def get_all(cls, **kwargs) -> List[dict]:
        """
        Return all models formatted as a list of dictionaries.
        """
        all_models = cls.get_all_models(**kwargs)
        return cls.schema().dump(all_models, many=True).data

    @classmethod
    def rollback_to(cls, **model_to_query) -> int:
        """
        All records matching the query and valid at specified validity will be considered as valid.
        :return Number of records updated.
        """
        return 0

    @classmethod
    def get_all_models(cls, **kwargs) -> list:
        """
        Return all SQLAlchemy models.
        """
        query = cls._session.query(cls)
        if 'order_by' in kwargs:
            query = query.order_by(*kwargs.pop('order_by'))
        for key, value in kwargs.items():
            if key == 'limit':
                query = query.limit(value)
            elif key == 'offset':
                query = query.offset(value)
            elif value is not None:
                query = query.filter(getattr(cls, key) == value)
        try:
            return query.all()
        except exc.sa_exc.DBAPIError:
            cls._handle_connection_failure()

    @classmethod
    def _handle_connection_failure(cls):
        """
        :raises Exception: Explaining that the database could not be reached.
        """
        logger.exception('Database could not be reached.')
        cls._session.close()  # Force connection close to properly re-establish it on next request
        raise Exception('Database could not be reached.')

    @classmethod
    def get(cls, **kwargs) -> dict:
        """
        Return the model formatted as a dictionary.
        """
        query = cls._session.query(cls)
        for key, value in kwargs.items():
            if value is not None:
                query = query.filter(getattr(cls, key) == value)
        try:
            model = query.one_or_none()
            return cls.schema().dump(model).data
        except exc.MultipleResultsFound:
            cls._session.rollback()  # SQLAlchemy state is not coherent with the reality if not rollback
            raise ValidationFailed(kwargs, message='More than one result: Consider another filtering.')
        except exc.sa_exc.DBAPIError:
            cls._handle_connection_failure()

    @classmethod
    def add_all(cls, models_as_list_of_dict: list) -> List[dict]:
        """
        Add models formatted as a list of dictionaries.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted models formatted as a list of dictionaries.
        """
        if not models_as_list_of_dict:
            raise ValidationFailed({}, message='No data provided.')
        try:
            models, errors = cls.schema().load(models_as_list_of_dict, many=True, session=cls._session)
        except exc.sa_exc.DBAPIError:
            cls._handle_connection_failure()
        if errors:
            raise ValidationFailed(models_as_list_of_dict, marshmallow_errors=errors)
        try:
            cls._session.add_all(models)
            if cls.audit_model:
                for inserted_dict in models_as_list_of_dict:
                    cls.audit_model.audit_add(inserted_dict)
            cls._session.commit()
            return _models_field_values(models)
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            cls._handle_connection_failure()
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def add(cls, model_as_dict: dict) -> dict:
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
            if cls.audit_model:
                cls.audit_model.audit_add(model_as_dict)
            ret = cls._session.commit()
            return _model_field_values(model)
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            cls._handle_connection_failure()
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def update(cls, model_as_dict: dict) -> (dict, dict):
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
            cls._handle_connection_failure()
        if not previous_model:
            raise ModelCouldNotBeFound(model_as_dict)
        previous_model_as_dict = _model_field_values(previous_model)
        new_model, errors = cls.schema().load(model_as_dict, instance=previous_model, partial=True,
                                              session=cls._session)
        if errors:
            raise ValidationFailed(model_as_dict, marshmallow_errors=errors)
        new_model_as_dict = _model_field_values(new_model)
        try:
            cls._session.add(new_model)
            if cls.audit_model:
                cls.audit_model.audit_update(new_model_as_dict)
            cls._session.commit()
            return previous_model_as_dict, new_model_as_dict
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            cls._handle_connection_failure()
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def remove(cls, **kwargs) -> int:
        """
        Remove the model(s) matching those criterion.
        :returns Number of removed rows.
        """
        try:
            query = cls._session.query(cls)
            for key, value in kwargs.items():
                if value is not None:
                    query = query.filter(getattr(cls, key) == value)
            if cls.audit_model:
                cls.audit_model.audit_remove(**kwargs)
            nb_removed = query.delete()
            cls._session.commit()
            return nb_removed
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            cls._handle_connection_failure()
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
                ordered = True

        schema = Schema(session=cls._session)
        mapper = inspect(cls)
        for attr in mapper.attrs:
            schema_field = schema.fields.get(attr.key, None)
            if schema_field:
                cls._enrich_schema_field(schema_field, attr)

        return schema

    @classmethod
    def _enrich_schema_field(cls, marshmallow_field, sql_alchemy_field):
        # Default value
        defaults = [column.default.arg for column in sql_alchemy_field.columns if column.default]
        if defaults:
            marshmallow_field.metadata['sqlalchemy_default'] = defaults[0]

        # Auto incremented field
        autoincrement = [column.autoincrement for column in sql_alchemy_field.columns if column.autoincrement]
        if autoincrement and isinstance(autoincrement[0], bool):
            marshmallow_field.metadata['sqlalchemy_autoincrement'] = autoincrement[0]

    @classmethod
    def query_get_parser(cls):
        query_get_parser = cls._query_parser()
        query_get_parser.add_argument('limit', type=inputs.positive)
        if _supports_offset(cls.metadata.bind.url.drivername):
            query_get_parser.add_argument('offset', type=inputs.natural)
        return query_get_parser

    @classmethod
    def query_delete_parser(cls):
        return cls._query_parser()

    @classmethod
    def query_rollback_parser(cls):
        return  # Only VersioningCRUDModel allows rollback

    @classmethod
    def _query_parser(cls):
        query_parser = reqparse.RequestParser()
        for marshmallow_field in cls.schema().fields.values():
            query_parser.add_argument(
                marshmallow_field.name,
                required=False,
                type=_get_python_type(marshmallow_field),
            )
        return query_parser

    @classmethod
    def description_dictionary(cls):
        description = {
            'table': cls.__tablename__,
        }

        if hasattr(cls, 'table_args__'):
            description['schema'] = cls.table_args__.get('schema')

        mapper = inspect(cls)
        for column in mapper.attrs:
            description[column.key] = column.columns[0].name

        return description

    @classmethod
    def flask_restplus_fields(cls, namespace):
        return {
            marshmallow_field.name: _get_rest_plus_type(marshmallow_field)(
                required=marshmallow_field.required,
                example=_get_example(marshmallow_field),
                description=marshmallow_field.metadata.get('description', None),
                enum=_get_choices(marshmallow_field),
                default=_get_default_value(marshmallow_field),
                readonly=_is_read_only_value(marshmallow_field)
            )
            for marshmallow_field in cls.schema().fields.values()
        }

    @classmethod
    def flask_restplus_description_fields(cls):
        exported_fields = {
            'table': flask_restplus_fields.String(required=True, example='table', description='Table name'),
        }

        if hasattr(cls, 'table_args__'):
            exported_fields['schema'] = flask_restplus_fields.String(required=True, example='schema',
                                                                     description='Table schema')

        exported_fields.update({
            marshmallow_field.name: flask_restplus_fields.String(
                required=marshmallow_field.required,
                example='column',
                description=marshmallow_field.metadata.get('description', None),
            )
            for marshmallow_field in cls.schema().fields.values()
        })
        return exported_fields

    @classmethod
    def audit(cls):
        """
        Call this method to add audit to a model.
        """
        from pycommon_database.audit_sqlalchemy import _create_from
        cls.audit_model = _create_from(cls)


def _load(database_connection_url: str, create_models_func: callable, **kwargs):
    """
    Create all necessary tables and perform the link between models and underlying database connection.

    :param database_connection_url: URL formatted as a standard database connection string (Mandatory).
    :param create_models_func: Function that will be called to create models and return them (instances of CRUDModel)
     (Mandatory).
    :param pool_recycle: Number of seconds to wait before recycling a connection pool. Default value is 60.
    """
    database_connection_url = _clean_database_url(database_connection_url)
    logger.info(f'Connecting to {database_connection_url}...')
    logger.debug(f'Creating engine...')
    if _in_memory(database_connection_url):
        engine = create_engine(database_connection_url, poolclass=StaticPool, connect_args={'check_same_thread': False})
    else:
        kwargs.setdefault('pool_recycle', 60)
        engine = create_engine(database_connection_url, **kwargs)
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
        if _in_memory(database_connection_url) and hasattr(base.metadata, '_schemas'):
            if len(base.metadata._schemas) > 1:
                raise MultiSchemaNotSupported()
            elif len(base.metadata._schemas) == 1:
                engine.execute(f"ATTACH DATABASE ':memory:' AS {next(iter(base.metadata._schemas))};")
        base.metadata.create_all(bind=engine)
        base.metadata.tables = all_tables_and_views
    logger.debug(f'Creating session...')
    session = sessionmaker(bind=engine)()
    logger.info(f'Connected to {database_connection_url}.')
    for model_class in model_classes:
        model_class._post_init(session)
    return base


def _reset(base):
    """
    If the database was already created, then drop all tables and recreate them all.
    """
    if base:
        logger.info(f'Resetting all data related to {base.metadata.bind.url}...')
        base.metadata.drop_all(bind=base.metadata.bind)
        base.metadata.create_all(bind=base.metadata.bind)
        logger.info(f'All data related to {base.metadata.bind.url} reset.')


def _model_field_values(model_instance):
    """Return model fields values (with the proper type) as a dictionary."""
    return model_instance.schema().dump(model_instance).data


def _models_field_values(model_instances: list):
    """Return models fields values (with the proper type) as a list of dictionaries."""
    if not model_instances:
        return []
    return model_instances[0].schema().dump(model_instances, many=True).data


class MultiSchemaNotSupported(Exception):
    def __init__(self):
        Exception.__init__(self, 'SQLite does not manage multi-schemas..')


def _clean_database_url(database_connection_url: str):
    connection_details = database_connection_url.split(':///?odbc_connect=', maxsplit=1)
    if len(connection_details) == 2:
        return f'{connection_details[0]}:///?odbc_connect={urllib.parse.quote_plus(connection_details[1])}'
    return database_connection_url


def _can_retrieve_metadata(database_connection_url: str):
    return not (database_connection_url.startswith('sybase') or
                database_connection_url.startswith('mssql'))


def _supports_offset(driver_name: str):
    return not (driver_name.startswith('sybase') or
                driver_name.startswith('mssql'))


def _in_memory(database_connection_url: str):
    return ':memory:' in database_connection_url


def _prepare_engine(engine):
    if engine.url.drivername.startswith('sybase'):
        engine.dialect.identifier_preparer.initial_quote = '['
        engine.dialect.identifier_preparer.final_quote = ']'


def _get_view_names(engine, schema) -> list:
    with engine.connect() as conn:
        return engine.dialect.get_view_names(conn, schema)


def _get_rest_plus_type(marshmallow_field):
    """
    Return the Flask RestPlus field type (as a class) corresponding to this SQL Alchemy Marshmallow field.

    :raises Exception if field type is not managed yet.
    """
    if isinstance(marshmallow_field, marshmallow_fields.String):
        return flask_restplus_fields.String
    if isinstance(marshmallow_field, marshmallow_fields.Integer):
        return flask_restplus_fields.Integer
    if isinstance(marshmallow_field, marshmallow_fields.Decimal):
        return flask_restplus_fields.Fixed
    if isinstance(marshmallow_field, marshmallow_fields.Float):
        return flask_restplus_fields.Float
    if isinstance(marshmallow_field, marshmallow_fields.Number):
        return flask_restplus_fields.Decimal
    if isinstance(marshmallow_field, marshmallow_fields.Boolean):
        return flask_restplus_fields.Boolean
    if isinstance(marshmallow_field, marshmallow_fields.Date):
        return flask_restplus_fields.Date
    if isinstance(marshmallow_field, marshmallow_fields.DateTime):
        return flask_restplus_fields.DateTime
    if isinstance(marshmallow_field, marshmallow_fields.Time):
        return flask_restplus_fields.DateTime
    # SQLAlchemy Enum fields will be converted to Marshmallow Raw Field
    if isinstance(marshmallow_field, marshmallow_fields.Field):
        return flask_restplus_fields.String

    raise Exception(f'Flask RestPlus field type cannot be guessed for {marshmallow_field} field.')


def _get_example(marshmallow_field) -> str:
    default_value = _get_default_value(marshmallow_field)
    if default_value:
        return str(default_value)

    choices = _get_choices(marshmallow_field)
    return str(choices[0]) if choices else _get_default_example(marshmallow_field)


def _get_choices(marshmallow_field):
    if marshmallow_field:
        for validator in marshmallow_field.validators:
            if isinstance(validator, validate.OneOf):
                return validator.choices


def _get_default_value(marshmallow_field):
    return marshmallow_field.metadata.get('sqlalchemy_default', None) if marshmallow_field else None


def _is_read_only_value(marshmallow_field) -> bool:
    return marshmallow_field.metadata.get('sqlalchemy_autoincrement', None) if marshmallow_field else None


def _get_default_example(marshmallow_field) -> str:
    """
    Return an Example value corresponding to this SQL Alchemy Marshmallow field.
    """
    if isinstance(marshmallow_field, marshmallow_fields.Integer):
        return '0'
    if isinstance(marshmallow_field, marshmallow_fields.Number):
        return '0.0'
    if isinstance(marshmallow_field, marshmallow_fields.Boolean):
        return 'true'
    if isinstance(marshmallow_field, marshmallow_fields.Date):
        return '2017-09-24'
    if isinstance(marshmallow_field, marshmallow_fields.DateTime):
        return '2017-09-24T15:36:09'
    if isinstance(marshmallow_field, marshmallow_fields.Time):
        return '15:36:09'
    if isinstance(marshmallow_field, marshmallow_fields.List):
        return 'xxxx'

    return 'sample_value'


def _get_python_type(marshmallow_field):
    """
    Return the Python type corresponding to this SQL Alchemy Marshmallow field.

    :raises Exception if field type is not managed yet.
    """
    if isinstance(marshmallow_field, marshmallow_fields.String):
        return str
    if isinstance(marshmallow_field, marshmallow_fields.Integer):
        return int
    if isinstance(marshmallow_field, marshmallow_fields.Number):
        return float
    if isinstance(marshmallow_field, marshmallow_fields.Boolean):
        return inputs.boolean
    if isinstance(marshmallow_field, marshmallow_fields.Date):
        return inputs.date_from_iso8601
    if isinstance(marshmallow_field, marshmallow_fields.DateTime):
        return inputs.datetime_from_iso8601
    if isinstance(marshmallow_field, marshmallow_fields.List):
        return list
    # SQLAlchemy Enum fields will be converted to Marshmallow Raw Field
    if isinstance(marshmallow_field, marshmallow_fields.Field):
        return str

    raise Exception(f'Python field type cannot be guessed for {marshmallow_field} field.')
