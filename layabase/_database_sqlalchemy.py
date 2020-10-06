import datetime
import logging
import urllib.parse
from typing import List, Dict, Type, Iterable
import operator

from marshmallow import ValidationError, EXCLUDE
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from sqlalchemy import create_engine, inspect, Column, text, or_, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, exc, PropComparator
from sqlalchemy.orm.query import Query
from sqlalchemy.pool import StaticPool
from sqlalchemy.engine.base import Engine

from layabase._exceptions import (
    MultiSchemaNotSupported,
    ValidationFailed,
    DatabaseError,
)
from layabase import ComparisonSigns, CRUDController


logger = logging.getLogger(__name__)


_operators = {
    ComparisonSigns.Greater: operator.gt,
    ComparisonSigns.GreaterOrEqual: operator.ge,
    ComparisonSigns.Lower: operator.lt,
    ComparisonSigns.LowerOrEqual: operator.le,
}


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
        cls._check_required_query_fields(filters)

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
                column: Column = getattr(cls, column_name)
                allow_like = column.info.get("layabase", {}).get(
                    "interpret_star_character", False
                )
                allow_comparison_signs = column.info.get("layabase", {}).get(
                    "allow_comparison_signs", False
                )
                equality_values = []
                comparison_filters = []
                column_filters = []

                for v in value if isinstance(value, list) else [value]:
                    if allow_like and isinstance(v, str) and "*" in v:
                        column_filters.append(column.like(v.replace("*", "%")))
                    elif allow_comparison_signs and isinstance(v, tuple):
                        comparison_filters.append(
                            column.operate(_operators[v[0]], v[1])
                        )
                    else:
                        equality_values.append(v)

                if len(comparison_filters) > 1:
                    column_filters.append(and_(*comparison_filters))
                elif comparison_filters:
                    column_filters.append(comparison_filters[0])

                if len(equality_values) > 1:
                    column_filters.append(column.in_(equality_values))
                elif equality_values:
                    column_filters.append(column == equality_values[0])

                if len(column_filters) > 1:
                    query = query.filter(or_(*column_filters))
                elif column_filters:
                    query = query.filter(column_filters[0])

        query = cls.customize_query(query)

        if query_limit:
            query = query.limit(query_limit)
        if query_offset:
            query = query.offset(query_offset)

        try:
            result = query.all()
            cls._session.close()
            return result
        except exc.sa_exc.DBAPIError as e:
            cls._handle_connection_failure(e)

    @classmethod
    def customize_query(cls, query: Query) -> Query:
        return query  # No custom behavior by default

    @classmethod
    def _handle_connection_failure(cls, exception: exc.sa_exc.DBAPIError):
        """
        :raises Exception: Explaining that the database could not be reached.
        """
        cls._session.close()  # Force connection close to properly re-establish it on next request
        raise DatabaseError(exception) from exception

    @classmethod
    def get(cls, **filters) -> dict:
        """
        Return the model formatted as a dictionary.
        """
        cls._check_required_query_fields(filters)
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
        except exc.sa_exc.DBAPIError as e:
            cls._handle_connection_failure(e)

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
        if not isinstance(rows, list):
            raise ValidationFailed(rows, message="Must be a list of dictionaries.")
        # TODO Check if it can be done by SQLAlchemy already
        rows = [cls._remove_auto_incremented_fields(row) for row in rows]
        try:
            models = cls.schema().load(rows, many=True, session=cls._session)
        except exc.sa_exc.DBAPIError as e:
            cls._handle_connection_failure(e)
        except ValidationError as e:
            raise ValidationFailed(rows, e.messages)
        try:
            cls._session.add_all(models)
            if cls.audit_model:
                for row in rows:
                    cls.audit_model.audit_add(row)
            cls._session.commit()
            return _models_field_values(models)
        except exc.sa_exc.DBAPIError as e:
            cls._session.rollback()
            cls._handle_connection_failure(e)
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

        row = cls._remove_auto_incremented_fields(row)
        try:
            model = cls.schema().load(row, session=cls._session)
        except exc.sa_exc.DBAPIError as e:
            raise DatabaseError(e) from e
        except ValidationError as e:
            raise ValidationFailed(row, e.messages)
        try:
            cls._session.add(model)
            if cls.audit_model:
                cls.audit_model.audit_add(row)
            cls._session.commit()
            return _model_field_values(model)
        except exc.sa_exc.DBAPIError as e:
            cls._session.rollback()
            cls._handle_connection_failure(e)
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def _remove_auto_incremented_fields(cls, row: dict) -> dict:
        if isinstance(row, dict):
            auto_incremented_fields = cls._get_auto_incremented_fields()
            return {
                name: value
                for name, value in row.items()
                if name not in auto_incremented_fields
            }

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
            if not isinstance(row, dict):
                raise ValidationFailed(row, message="Must be a dictionary.")
            try:
                previous_model = cls.schema().get_instance(row)
            except exc.sa_exc.DBAPIError as e:
                cls._handle_connection_failure(e)
            if not previous_model:
                raise ValidationFailed(
                    row, message="The row to update could not be found."
                )
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
        except exc.sa_exc.DBAPIError as e:
            cls._session.rollback()
            cls._handle_connection_failure(e)
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
        if not isinstance(row, dict):
            raise ValidationFailed(row, message="Must be a dictionary.")
        try:
            previous_model = cls.schema().get_instance(row)
        except exc.sa_exc.DBAPIError as e:
            cls._handle_connection_failure(e)
        if not previous_model:
            raise ValidationFailed(row, message="The row to update could not be found.")
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
        except exc.sa_exc.DBAPIError as e:
            cls._session.rollback()
            cls._handle_connection_failure(e)
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def remove(cls, **filters) -> int:
        """
        Remove the model(s) matching those criterion.

        :returns Number of removed rows.
        """
        cls._check_required_query_fields(filters)
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
        except exc.sa_exc.DBAPIError as e:
            cls._session.rollback()
            cls._handle_connection_failure(e)
        except Exception:
            cls._session.rollback()
            raise

    @classmethod
    def schema(cls) -> SQLAlchemyAutoSchema:
        """
        Create a new Marshmallow SQL Alchemy schema instance.
        TODO Remove the need for a new schema instance every time. Create it once and for all

        :return: The newly created schema instance.
        """

        class Schema(SQLAlchemyAutoSchema):
            class Meta:
                model = cls
                ordered = True
                unknown = EXCLUDE
                load_instance = True

        return Schema(session=cls._session)

    @classmethod
    def get_primary_keys(cls) -> List[str]:
        # TODO Replace with marshmallow_sqlalchemy.fields.get_primary_keys(cls)
        return [
            marshmallow_field.name
            for marshmallow_field in cls.schema().fields.values()
            if marshmallow_field.required
        ]

    @classmethod
    def _get_auto_incremented_fields(cls) -> List[str]:
        mapper = inspect(cls)
        return [
            column.name
            for column in mapper.columns.values()
            if column.autoincrement is True
        ]

    @classmethod
    def _check_required_query_fields(cls, filters):
        for required_field in cls._get_required_query_fields():
            if required_field not in filters:
                raise ValidationFailed(
                    filters,
                    errors={required_field: ["Missing data for required field."]},
                )

    @classmethod
    def _get_required_query_fields(cls) -> List[str]:
        return [
            name
            for name, column in cls.__dict__.items()
            if isinstance(column, PropComparator)
            and column.info.get("layabase", {}).get("required_on_query", False)
        ]

    @classmethod
    def description_dictionary(cls) -> Dict[str, str]:
        description = {"table": cls.__tablename__}

        if hasattr(cls, "__table_args__"):
            description["schema"] = cls.__table_args__.get("schema")

        mapper = inspect(cls)
        for column in mapper.attrs:
            description[column.key] = column.columns[0].name

        return description

    @classmethod
    def get_field_names(cls) -> List[str]:
        return [field.name for field in cls.schema().fields.values()]


def _create_model(controller: CRUDController, base) -> Type[CRUDModel]:
    model: Type[CRUDModel] = type(
        f"{controller.table_or_collection.__name__}_SQLAlchemyModel",
        (controller.table_or_collection, CRUDModel, base),
        {},
    )

    controller._model = model

    controller.supports_offset = _supports_offset(base.metadata.bind.url.drivername)

    if controller.audit:
        from layabase._audit_sqlalchemy import _create_from, _to_audit_column

        table_copy = type(
            f"{controller.table_or_collection.__name__}_Copy_For_Audit",
            controller.table_or_collection.__bases__,
            {
                key: _to_audit_column(value)
                for key, value in controller.table_or_collection.__dict__.items()
                if key not in ["__dict__", "__weakref__"]
            },
        )

        model.audit_model = type(
            f"{controller.table_or_collection.__name__}_SQLAlchemyAuditModel",
            (
                _create_from(model, controller.retrieve_user),
                table_copy,
                CRUDModel,
                base,
            ),
            {"__tablename__": f"audit_{controller.table_or_collection.__tablename__}"},
        )

    controller._model_description_dictionary = model.description_dictionary()

    return model


def _load(
    database_connection_url: str, controllers: Iterable[CRUDController], **kwargs
):
    """
    Create all necessary tables and perform the link between models and underlying database connection.

    :param database_connection_url: URL formatted as a standard database connection string (Mandatory).
    :param controllers: List of all CRUDController-like instances (Mandatory).
    :param pool_recycle: Number of seconds to wait before recycling a connection pool. Default value is 60.
    :param base_parameters: Dictionary containing the parameters that will be sent for base creation.
    :return SQLAlchemy base.
    """
    database_connection_url = _clean_database_url(database_connection_url)
    logger.info(f"Connecting to {database_connection_url}...")
    logger.debug("Creating engine...")
    base_parameters = kwargs.pop("base_parameters", None) or {}
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
    logger.debug("Creating base...")
    base = declarative_base(bind=engine, **base_parameters)
    logger.debug("Creating models...")
    model_classes = [_create_model(controller, base) for controller in controllers]
    if _can_retrieve_metadata(database_connection_url):
        all_view_names = _get_view_names(engine, base.metadata.schema)
        all_tables_and_views = base.metadata.tables
        # Remove all views from table list before creating them
        base.metadata.tables = {
            table_name: table_or_view
            for table_name, table_or_view in all_tables_and_views.items()
            if table_name not in all_view_names
        }
        logger.debug("Creating tables...")
        if _in_memory(database_connection_url) and hasattr(base.metadata, "_schemas"):
            if len(base.metadata._schemas) > 1:
                raise MultiSchemaNotSupported()
            elif len(base.metadata._schemas) == 1:
                engine.execute(
                    f"ATTACH DATABASE ':memory:' AS {next(iter(base.metadata._schemas))};"
                )
        base.metadata.create_all(bind=engine)
        base.metadata.tables = all_tables_and_views
    logger.debug("Creating session...")
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
    return model_instances[0].schema().dump(model_instances, many=True)


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


def _prepare_engine(engine: Engine):
    if engine.url.drivername.startswith("sybase"):
        engine.dialect.identifier_preparer.initial_quote = "["
        engine.dialect.identifier_preparer.final_quote = "]"


def _get_view_names(engine: Engine, schema: str) -> list:
    """Return a list of view names, upper cased and prefixed by schema if needed."""
    with engine.connect() as conn:
        return [
            f"{f'{schema}.' if schema else ''}{view_name.upper()}"
            for view_name in engine.dialect.get_view_names(conn, schema)
        ]


def _check(base) -> (str, dict):
    """
    Return Health checks for this SqlAlchemy database.

    :param base: database object as returned by the _load method (Mandatory).
    :return: A tuple with a string providing the status (pass, warn, fail), and the checks.
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
