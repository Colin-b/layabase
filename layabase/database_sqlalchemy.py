import datetime
import logging
import urllib.parse
from typing import List, Dict

from flask_restplus import fields as flask_restplus_fields, reqparse, inputs
from marshmallow import validate, ValidationError, EXCLUDE
from marshmallow_sqlalchemy import ModelSchema
from marshmallow_sqlalchemy.fields import fields as marshmallow_fields
from layaberr import ValidationFailed, ModelCouldNotBeFound
from sqlalchemy import create_engine, inspect, Column, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, exc
from sqlalchemy.pool import StaticPool

logger = logging.getLogger(__name__)


class CRUDModel:
    """
    Class providing CRUD helper methods for a SQL Alchemy model.
    _session class property must be specified in Model.
    Calling load_from(...) will provide you one.
    """

    _session = None
    audit_model = None
    interpret_star_character = False

    @classmethod
    def _post_init(cls, session):
        cls._session = session
        if cls.audit_model:
            cls.audit_model._post_init(session)

    @classmethod
    def get_all(cls, **filters) -> List[dict]:
        """
        Return all models formatted as a list of dictionaries.
        """
        rows = cls.get_all_models(**filters)
        return cls.schema().dump(rows, many=True)

    @classmethod
    def get_history(cls, **filters) -> List[dict]:
        return cls.get_all(**filters)

    @classmethod
    def rollback_to(cls, **filters) -> int:
        """
        All records matching the query and valid at specified validity will be considered as valid.

        :return Number of records updated.
        """
        return 0

    @classmethod
    def get_all_models(cls, **filters) -> list:
        """
        Return all SQLAlchemy models.
        """
        query = cls._session.query(cls)

        order_by = filters.pop("order_by", [])
        if order_by:
            query = query.order_by(
                *[
                    text(column) if isinstance(column, str) else column
                    for column in order_by
                ]
            )

        query_limit = filters.pop("limit", None)
        query_offset = filters.pop("offset", None)

        for column_name, value in filters.items():
            if value is not None:
                if isinstance(value, list):
                    if value:
                        query = query.filter(getattr(cls, column_name).in_(value))
                else:
                    if (
                        cls.interpret_star_character
                        and isinstance(value, str)
                        and "*" in value
                    ):
                        query = query.filter(
                            getattr(cls, column_name).like(value.replace("*", "%"))
                        )
                    else:
                        query = query.filter(getattr(cls, column_name) == value)

        if query_limit:
            query = query.limit(query_limit)
        if query_offset:
            query = query.offset(query_offset)

        try:
            result = query.all()
            cls._session.close()
            return result
        except exc.sa_exc.DBAPIError:
            cls._handle_connection_failure()

    @classmethod
    def _handle_connection_failure(cls):
        """
        :raises Exception: Explaining that the database could not be reached.
        """
        logger.exception("Database could not be reached.")
        cls._session.close()  # Force connection close to properly re-establish it on next request
        raise Exception("Database could not be reached.")

    @classmethod
    def get(cls, **filters) -> dict:
        """
        Return the model formatted as a dictionary.
        """
        query = cls._session.query(cls)
        for column_name, value in filters.items():
            if value is not None:
                if isinstance(value, list):
                    if not value:
                        continue
                    if len(value) > 1:
                        raise ValidationFailed(
                            filters, {column_name: ["Only one value must be queried."]}
                        )
                    value = value[0]
                query = query.filter(getattr(cls, column_name) == value)
        try:
            model = query.one_or_none()
            cls._session.close()
            return cls.schema().dump(model)
        except exc.MultipleResultsFound:
            cls._session.rollback()  # SQLAlchemy state is not coherent with the reality if not rollback
            raise ValidationFailed(
                filters, message="More than one result: Consider another filtering."
            )
        except exc.sa_exc.DBAPIError:
            cls._handle_connection_failure()

    @classmethod
    def get_last(cls, **filters) -> dict:
        """
        Return last revision of the model formatted as a dictionary.
        """
        return cls.get(**filters)

    @classmethod
    def add_all(cls, rows: List[dict]) -> List[dict]:
        """
        Add models formatted as a list of dictionaries.

        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted models formatted as a list of dictionaries.
        """
        if not rows:
            raise ValidationFailed({}, message="No data provided.")
        try:
            models = cls.schema().load(rows, many=True, session=cls._session)
        except exc.sa_exc.DBAPIError:
            cls._handle_connection_failure()
        except ValidationError as e:
            raise ValidationFailed(rows, e.messages)
        try:
            cls._session.add_all(models)
            if cls.audit_model:
                for row in rows:
                    cls.audit_model.audit_add(row)
            cls._session.commit()
            return _models_field_values(models)
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            cls._handle_connection_failure()
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def add(cls, row: dict) -> dict:
        """
        Add a model formatted as a dictionary.

        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted model formatted as a dictionary.
        """
        if not row:
            raise ValidationFailed({}, message="No data provided.")
        try:
            model = cls.schema().load(row, session=cls._session)
        except exc.sa_exc.DBAPIError:
            logger.exception("Database could not be reached.")
            raise Exception("Database could not be reached.")
        except ValidationError as e:
            raise ValidationFailed(row, e.messages)
        try:
            cls._session.add(model)
            if cls.audit_model:
                cls.audit_model.audit_add(row)
            cls._session.commit()
            return _model_field_values(model)
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            cls._handle_connection_failure()
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def update_all(cls, rows: List[dict]) -> (List[dict], List[dict]):
        """
        Update models formatted as a list of dictionaries.

        :raises ValidationFailed in case Marshmallow validation fail.
        :returns A tuple containing previous models formatted as a list of dictionaries (first item)
        and new models formatted as a list of dictionaries (second item).
        """
        if not rows:
            raise ValidationFailed({}, message="No data provided.")
        previous_rows = []
        new_rows = []
        new_models = []
        for row in rows:
            try:
                previous_model = cls.schema().get_instance(row)
            except exc.sa_exc.DBAPIError:
                cls._handle_connection_failure()
            if not previous_model:
                raise ModelCouldNotBeFound(row)
            previous_row = _model_field_values(previous_model)
            try:
                new_model = cls.schema().load(
                    row, instance=previous_model, partial=True, session=cls._session
                )
            except ValidationError as e:
                raise ValidationFailed(row, e.messages)
            new_row = _model_field_values(new_model)

            previous_rows.append(previous_row)
            new_rows.append(new_row)
            new_models.append(new_model)

        try:
            cls._session.add_all(new_models)
            if cls.audit_model:
                for new_row in new_rows:
                    cls.audit_model.audit_update(new_row)
            cls._session.commit()
            return previous_rows, new_rows
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            cls._handle_connection_failure()
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def update(cls, row: dict) -> (dict, dict):
        """
        Update a model formatted as a dictionary.

        :raises ValidationFailed in case Marshmallow validation fail.
        :returns A tuple containing previous model formatted as a dictionary (first item)
        and new model formatted as a dictionary (second item).
        """
        if not row:
            raise ValidationFailed({}, message="No data provided.")
        try:
            previous_model = cls.schema().get_instance(row)
        except exc.sa_exc.DBAPIError:
            cls._handle_connection_failure()
        if not previous_model:
            raise ModelCouldNotBeFound(row)
        previous_row = _model_field_values(previous_model)
        try:
            new_model = cls.schema().load(
                row, instance=previous_model, partial=True, session=cls._session
            )
        except ValidationError as e:
            raise ValidationFailed(row, e.messages)
        new_row = _model_field_values(new_model)
        try:
            cls._session.add(new_model)
            if cls.audit_model:
                cls.audit_model.audit_update(new_row)
            cls._session.commit()
            return previous_row, new_row
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            cls._handle_connection_failure()
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def remove(cls, **filters) -> int:
        """
        Remove the model(s) matching those criterion.

        :returns Number of removed rows.
        """
        try:
            query = cls._session.query(cls)
            for column_name, value in filters.items():
                if value is not None:
                    if isinstance(value, list):
                        if value:
                            query = query.filter(getattr(cls, column_name).in_(value))
                    else:
                        query = query.filter(getattr(cls, column_name) == value)
            if cls.audit_model:
                cls.audit_model.audit_remove(**filters)
            nb_removed = query.delete(synchronize_session="fetch")
            cls._session.commit()
            return nb_removed
        except exc.sa_exc.DBAPIError:
            cls._session.rollback()
            cls._handle_connection_failure()
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def schema(cls) -> ModelSchema:
        """
        Create a new Marshmallow SQL Alchemy schema instance.

        :return: The newly created schema instance.
        """

        class Schema(ModelSchema):
            class Meta:
                model = cls
                ordered = True
                unknown = EXCLUDE

        schema = Schema(session=cls._session)
        mapper = inspect(cls)
        for attr in mapper.attrs:
            schema_field = schema.fields.get(attr.key, None)
            if schema_field:
                cls._enrich_schema_field(schema_field, attr)

        return schema

    @classmethod
    def _enrich_schema_field(
        cls, marshmallow_field: marshmallow_fields.Field, sql_alchemy_field: Column
    ):
        # Default value
        defaults = [
            column.default.arg for column in sql_alchemy_field.columns if column.default
        ]
        if defaults:
            marshmallow_field.metadata["sqlalchemy_default"] = defaults[0]

        # Auto incremented field
        autoincrement = [
            column.autoincrement
            for column in sql_alchemy_field.columns
            if column.autoincrement
        ]
        if autoincrement and isinstance(autoincrement[0], bool):
            marshmallow_field.metadata["sqlalchemy_autoincrement"] = autoincrement[0]

    @classmethod
    def query_get_parser(cls) -> reqparse.RequestParser:
        query_get_parser = cls._query_parser()
        query_get_parser.add_argument("limit", type=inputs.positive)
        query_get_parser.add_argument("order_by", type=str, action="append")
        if _supports_offset(cls.metadata.bind.url.drivername):
            query_get_parser.add_argument("offset", type=inputs.natural)
        return query_get_parser

    @classmethod
    def query_get_history_parser(cls) -> reqparse.RequestParser:
        query_get_hist_parser = cls._query_parser()
        query_get_hist_parser.add_argument("limit", type=inputs.positive)
        if _supports_offset(cls.metadata.bind.url.drivername):
            query_get_hist_parser.add_argument("offset", type=inputs.natural)
        return query_get_hist_parser

    @classmethod
    def query_delete_parser(cls) -> reqparse.RequestParser:
        return cls._query_parser()

    @classmethod
    def query_rollback_parser(cls) -> None:
        return  # Only VersionedCRUDModel allows rollback

    @classmethod
    def _query_parser(cls) -> reqparse.RequestParser:
        query_parser = reqparse.RequestParser()
        for marshmallow_field in cls.schema().fields.values():
            query_parser.add_argument(
                marshmallow_field.name,
                required=False,
                type=_get_python_type(marshmallow_field),
                action="append",
            )
        return query_parser

    @classmethod
    def get_primary_keys(cls) -> List[str]:
        return [
            marshmallow_field.name
            for marshmallow_field in cls.schema().fields.values()
            if marshmallow_field.required
        ]

    @classmethod
    def description_dictionary(cls) -> Dict[str, str]:
        description = {"table": cls.__tablename__}

        if hasattr(cls, "table_args__"):
            description["schema"] = cls.table_args__.get("schema")

        mapper = inspect(cls)
        for column in mapper.attrs:
            description[column.key] = column.columns[0].name

        return description

    @classmethod
    def json_post_model(cls, namespace):
        return cls._model_with_all_fields(namespace)

    @classmethod
    def json_put_model(cls, namespace):
        return cls._model_with_all_fields(namespace)

    @classmethod
    def get_response_model(cls, namespace):
        return cls._model_with_all_fields(namespace)

    @classmethod
    def get_history_response_model(cls, namespace):
        return cls._model_with_all_fields(namespace)

    @classmethod
    def _model_with_all_fields(cls, namespace):
        return namespace.model(cls.__name__, cls._flask_restplus_fields())

    @classmethod
    def _flask_restplus_fields(cls) -> dict:
        return {
            marshmallow_field.name: _get_rest_plus_type(marshmallow_field)(
                required=marshmallow_field.required,
                example=_get_example(marshmallow_field),
                description=marshmallow_field.metadata.get("description", None),
                enum=_get_choices(marshmallow_field),
                default=_get_default_value(marshmallow_field),
                readonly=_is_read_only_value(marshmallow_field),
            )
            for marshmallow_field in cls.schema().fields.values()
        }

    @classmethod
    def get_field_names(cls) -> List[str]:
        return [field.name for field in cls.schema().fields.values()]

    @classmethod
    def flask_restplus_description_fields(cls) -> dict:
        exported_fields = {
            "table": flask_restplus_fields.String(
                required=True, example="table", description="Table name"
            )
        }

        if hasattr(cls, "table_args__"):
            exported_fields["schema"] = flask_restplus_fields.String(
                required=True, example="schema", description="Table schema"
            )

        exported_fields.update(
            {
                marshmallow_field.name: flask_restplus_fields.String(
                    required=marshmallow_field.required,
                    example="column",
                    description=marshmallow_field.metadata.get("description", None),
                )
                for marshmallow_field in cls.schema().fields.values()
            }
        )
        return exported_fields

    @classmethod
    def audit(cls) -> None:
        """
        Call this method to add audit to a model.
        """
        from layabase.audit_sqlalchemy import _create_from

        cls.audit_model = _create_from(cls)

    @classmethod
    def interpret_star_character_as_like(cls) -> None:
        """
        Call this method to interpret star character for LIKE operator.
        """

        cls.interpret_star_character = True


def _load(database_connection_url: str, create_models_func: callable, **kwargs):
    """
    Create all necessary tables and perform the link between models and underlying database connection.

    :param database_connection_url: URL formatted as a standard database connection string (Mandatory).
    :param create_models_func: Function that will be called to create models and return them (instances of CRUDModel)
     (Mandatory).
    :param pool_recycle: Number of seconds to wait before recycling a connection pool. Default value is 60.
    :return SQLAlchemy base.
    """
    database_connection_url = _clean_database_url(database_connection_url)
    logger.info(f"Connecting to {database_connection_url}...")
    logger.debug(f"Creating engine...")
    if _in_memory(database_connection_url):
        engine = create_engine(
            database_connection_url,
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )
    else:
        kwargs.setdefault("pool_recycle", 60)
        engine = create_engine(database_connection_url, **kwargs)
    _prepare_engine(engine)
    logger.debug(f"Creating base...")
    base = declarative_base(bind=engine)
    logger.debug(f"Creating models...")
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
        logger.debug(f"Creating tables...")
        if _in_memory(database_connection_url) and hasattr(base.metadata, "_schemas"):
            if len(base.metadata._schemas) > 1:
                raise MultiSchemaNotSupported()
            elif len(base.metadata._schemas) == 1:
                engine.execute(
                    f"ATTACH DATABASE ':memory:' AS {next(iter(base.metadata._schemas))};"
                )
        base.metadata.create_all(bind=engine)
        base.metadata.tables = all_tables_and_views
    logger.debug(f"Creating session...")
    session = sessionmaker(bind=engine)()
    logger.info(f"Connected to {database_connection_url}.")
    for model_class in model_classes:
        model_class._post_init(session)
    return base


def _reset(base) -> None:
    """
    If the database was already created, then drop all tables and recreate them all.
    """
    if base:
        logger.info(f"Resetting all data related to {base.metadata.bind.url}...")
        base.metadata.drop_all(bind=base.metadata.bind)
        base.metadata.create_all(bind=base.metadata.bind)
        logger.info(f"All data related to {base.metadata.bind.url} reset.")


def _model_field_values(model_instance) -> dict:
    """Return model fields values (with the proper type) as a dictionary."""
    return model_instance.schema().dump(model_instance)


def _models_field_values(model_instances: list) -> List[dict]:
    """Return models fields values (with the proper type) as a list of dictionaries."""
    if not model_instances:
        return []
    return model_instances[0].schema().dump(model_instances, many=True)


class MultiSchemaNotSupported(Exception):
    def __init__(self):
        Exception.__init__(self, "SQLite does not manage multi-schemas..")


def _clean_database_url(database_connection_url: str) -> str:
    connection_details = database_connection_url.split(":///?odbc_connect=", maxsplit=1)
    if len(connection_details) == 2:
        return f"{connection_details[0]}:///?odbc_connect={urllib.parse.quote_plus(connection_details[1])}"
    return database_connection_url


def _can_retrieve_metadata(database_connection_url: str) -> bool:
    return not (
        database_connection_url.startswith("sybase")
        or database_connection_url.startswith("mssql")
    )


def _supports_offset(driver_name: str) -> bool:
    return not (driver_name.startswith("sybase") or driver_name.startswith("mssql"))


def _in_memory(database_connection_url: str) -> bool:
    return ":memory:" in database_connection_url


def _prepare_engine(engine):
    if engine.url.drivername.startswith("sybase"):
        engine.dialect.identifier_preparer.initial_quote = "["
        engine.dialect.identifier_preparer.final_quote = "]"


def _get_view_names(engine, schema) -> list:
    with engine.connect() as conn:
        return engine.dialect.get_view_names(conn, schema)


def _get_rest_plus_type(marshmallow_field):
    """
    Return the Flask RestPlus field type (as a class) corresponding to this SQL Alchemy Marshmallow field.
    Default to String field.
    TODO Faster to use a dict from type to field ?
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
    return flask_restplus_fields.String


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
    return (
        marshmallow_field.metadata.get("sqlalchemy_default", None)
        if marshmallow_field
        else None
    )


def _is_read_only_value(marshmallow_field) -> bool:
    return (
        marshmallow_field.metadata.get("sqlalchemy_autoincrement", None)
        if marshmallow_field
        else None
    )


def _get_default_example(marshmallow_field) -> str:
    """
    Return an Example value corresponding to this SQL Alchemy Marshmallow field.
    """
    if isinstance(marshmallow_field, marshmallow_fields.Integer):
        return "0"
    if isinstance(marshmallow_field, marshmallow_fields.Number):
        return "0.0"
    if isinstance(marshmallow_field, marshmallow_fields.Boolean):
        return "true"
    if isinstance(marshmallow_field, marshmallow_fields.Date):
        return "2017-09-24"
    if isinstance(marshmallow_field, marshmallow_fields.DateTime):
        return "2017-09-24T15:36:09"
    if isinstance(marshmallow_field, marshmallow_fields.Time):
        return "15:36:09"
    if isinstance(marshmallow_field, marshmallow_fields.List):
        return "xxxx"

    return "sample_value"


def _get_python_type(marshmallow_field):
    """
    Return the Python type corresponding to this SQL Alchemy Marshmallow field.

    Default to str,
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
    return str


def _health_details(base) -> (str, dict):
    """
    Return Health details for this SqlAlchemy database.

    :param base: database object as returned by the _load method (Mandatory).
    :return: A tuple with a string providing the status (pass, warn, fail), and the details.
    """
    try:
        if base.metadata.bind.dialect.do_ping(base.metadata.bind.connect().connection):
            return (
                "pass",
                {
                    f"{base.metadata.bind.engine.name}:select": {
                        "componentType": "datastore",
                        "observedValue": "",
                        "status": "pass",
                        "time": datetime.datetime.utcnow().isoformat(),
                    }
                },
            )
        return (
            "fail",
            {
                f"{base.metadata.bind.engine.name}:select": {
                    "componentType": "datastore",
                    "status": "fail",
                    "time": datetime.datetime.utcnow().isoformat(),
                    "output": "Unable to ping database.",
                }
            },
        )
    except Exception as e:
        return (
            "fail",
            {
                f"{base.metadata.bind.engine.name}:select": {
                    "componentType": "datastore",
                    "status": "fail",
                    "time": datetime.datetime.utcnow().isoformat(),
                    "output": str(e),
                }
            },
        )
