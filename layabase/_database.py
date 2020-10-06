import enum
import logging
from typing import List, Union, Iterable

from layabase._exceptions import ControllerModelNotSet, ValidationFailed

logger = logging.getLogger(__name__)


@enum.unique
class ComparisonSigns(enum.Enum):
    GreaterOrEqual = ">="
    Greater = ">"
    LowerOrEqual = "<="
    Lower = "<"

    def __init__(self, value: str):
        self.length = len(value)

    @classmethod
    def deserialize(cls, value: str) -> Union[tuple, str]:
        """
        Convert a string representation of a sign and its value to tuple. Handle the fact that there may not be a sign.

        >>> ComparisonSigns.deserialize("test")
        'test'

        >>> ComparisonSigns.deserialize(">=test")
        (<ComparisonSigns.GreaterOrEqual: '>='>, 'test')

        >>> ComparisonSigns.deserialize("<test")
        (<ComparisonSigns.Lower: '<'>, 'test')
        """
        for sign in cls:
            if value.startswith(sign.value):
                return sign, value[sign.length :]

        return value


class NoDatabaseProvided(Exception):
    def __init__(self):
        Exception.__init__(self, "A database connection URL must be provided.")


class NoRelatedControllers(Exception):
    def __init__(self):
        Exception.__init__(self, "A list of CRUDController must be provided.")


def check(base) -> (str, dict):
    """
    Return Health "Checks object" for this database connection.

    :param base: database object as returned by the load method (Mandatory).
    :return: A tuple with a string providing the status (pass, warn, fail), and the "Checks object".
    Based on https://inadarei.github.io/rfc-healthcheck/
    """
    if not base:
        raise NoDatabaseProvided()

    if hasattr(base, "is_mongos"):
        from layabase._database_mongo import _check
    else:
        from layabase._database_sqlalchemy import _check

    return _check(base)


class CRUDController:
    """
    Class providing methods to interact with a Table or a Mongo Collection.
    """

    def __init__(self, table_or_collection, **kwargs):
        """
        Create a new controller to manipulate a table or mongo collection.

        :param table_or_collection: Naive python class describing a table or a mongo collection.
        :param history: True to be able to rollback to any state in the past. No history by default. (Mongo only)
        :param audit: True to keep record of every action on the underlying table or collection. No audit by default.
        :param skip_name_check: True to be able to force the usage of forbidden table or collection names. Name check is enforced by default. (Mongo only)
        :param skip_unknown_fields: False to use strict field name check. Ignore unknown fields by default. (Mongo only)
        :param skip_update_indexes: True to never update indexes. Warning, this might lead to invalid indexes on the underlying table or collection. (Mongo only)
        :param skip_log_for_unknown_fields: List of unknown field names that are to be expected.
        :param retrieve_user: Callable returning the user to store in case of audit.
        """
        if not table_or_collection:
            raise Exception("Table or Collection must be provided.")

        self.table_or_collection = table_or_collection
        self.history = kwargs.pop("history", False)
        self.audit = kwargs.pop("audit", False)
        self.skip_name_check = kwargs.pop("skip_name_check", False)
        self.skip_unknown_fields = kwargs.pop("skip_unknown_fields", True)
        self.skip_update_indexes = kwargs.pop("skip_update_indexes", False)
        self.skip_log_for_unknown_fields = kwargs.pop("skip_log_for_unknown_fields", [])
        self.supports_offset = True
        # By default, audit user will be blank
        self.retrieve_user = kwargs.pop("retrieve_user", lambda: "")

        # Generated from table_or_collection, appropriate class depending on what was requested on controller
        self._model = None

        # The response that is always sent for the Model Description
        self._model_description_dictionary = None

    @property
    def flask_restx(self):
        from layabase._flask_restx import ParsersAndModels

        if not hasattr(self, "_flask_restx"):
            self._flask_restx = ParsersAndModels(
                self.table_or_collection, self.history, self.audit, self.supports_offset
            )
        return self._flask_restx

    def get(self, request_arguments: dict) -> List[dict]:
        """
        Return all models formatted as a list of dictionaries.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        if not isinstance(request_arguments, dict):
            raise ValidationFailed(request_arguments, message="Must be a dictionary.")
        return self._model.get_all(**request_arguments)

    def get_one(self, request_arguments: dict) -> dict:
        """
        Return a model formatted as a dictionary.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        if not isinstance(request_arguments, dict):
            raise ValidationFailed(request_arguments, message="Must be a dictionary.")
        return self._model.get(**request_arguments)

    def get_last(self, request_arguments: dict) -> dict:
        """
        Return last revision of a model formatted as a dictionary.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        if not isinstance(request_arguments, dict):
            raise ValidationFailed(request_arguments, message="Must be a dictionary.")
        return self._model.get_last(**request_arguments)

    def get_url(self, endpoint: str, *new_dicts) -> str:
        """
        Return URL providing dictionaries.

        :param endpoint: Original GET endpoint (without any query parameter).
        :param new_dicts: All dictionaries that should be returned via the endpoint.
        """
        if not self._model:
            raise ControllerModelNotSet(self)

        if not new_dicts:
            return endpoint

        dict_identifiers = [
            f"{primary_key}={new_dict[primary_key]}"
            for new_dict in new_dicts
            for primary_key in self._model.get_primary_keys()
        ]
        return (
            f'{endpoint}{"?" if dict_identifiers else ""}{"&".join(dict_identifiers)}'
        )

    def post(self, new_dict: dict) -> dict:
        """
        Add a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted model formatted as a dictionary.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        return self._model.add(new_dict)

    def post_many(self, new_dicts: List[dict]) -> List[dict]:
        """
        Add models formatted as a list of dictionaries.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted models formatted as a list of dictionaries.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        return self._model.add_all(new_dicts)

    def put(self, updated_dict: dict) -> (dict, dict):
        """
        Update a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns A tuple containing previous model formatted as a dictionary (first item)
        and new model formatted as a dictionary (second item).
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        return self._model.update(updated_dict)

    def put_many(self, updated_dicts: List[dict]) -> (List[dict], List[dict]):
        """
        Update models formatted as a list of dictionaries.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns A tuple containing previous models formatted as a list of dictionaries (first item)
        and new models formatted as a list of dictionaries (second item).
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        return self._model.update_all(updated_dicts)

    def delete(self, request_arguments: dict) -> int:
        """
        Remove the model(s) matching those criterion.
        :returns Number of removed rows.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        if not isinstance(request_arguments, dict):
            raise ValidationFailed(request_arguments, message="Must be a dictionary.")
        return self._model.remove(**request_arguments)

    def get_audit(self, request_arguments: dict) -> List[dict]:
        """
        Return all audit models formatted as a list of dictionaries.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        if not self._model.audit_model:
            return []
        if not isinstance(request_arguments, dict):
            raise ValidationFailed(request_arguments, message="Must be a dictionary.")
        return self._model.audit_model.get_all(**request_arguments)

    def get_model_description(self) -> dict:
        if not self._model_description_dictionary:
            raise ControllerModelNotSet(self)
        return self._model_description_dictionary

    def rollback_to(self, request_arguments: dict) -> int:
        """
        Rollback to the model(s) matching those criterion.
        :returns Number of affected rows.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        if not isinstance(request_arguments, dict):
            raise ValidationFailed(request_arguments, message="Must be a dictionary.")
        return self._model.rollback_to(**request_arguments)

    def get_history(self, request_arguments: dict) -> List[dict]:
        """
        Return all models formatted as a list of dictionaries.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        if not isinstance(request_arguments, dict):
            raise ValidationFailed(request_arguments, message="Must be a dictionary.")
        return self._model.get_history(**request_arguments)

    def get_field_names(self) -> List[str]:
        """
        Return all model field names formatted as a str list.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        return self._model.get_field_names()


def load(database_connection_url: str, controllers: Iterable[CRUDController], **kwargs):
    """
    Create all necessary tables and perform the link between models and underlying database connection.

    :param database_connection_url: URL formatted as a standard database connection string (Mandatory).
    Here are some sample connection urls:
     - Mongo (in memory): mongomock
     - Mongo: mongodb://host:port/server_name

     - SQL Lite (in memory): sqlite:///:memory:
     - Postgre SQL: postgresql://user_name:user_password@host:port/server_name
     - Oracle: oracle://user_name:user_password@host:port/server_name
     - Sybase: sybase+pyodbc:///?odbc_connect=DRIVER={FreeTDS};TDS_Version=5.0;Server=host;Port=port;Database=server_name;UID=user_name;PWD=user_password;APP=sybase_application_name
    :param controllers: List of CRUDController-like instances (Mandatory).
    :param kwargs: Additional custom parameters:
     In case database connection URL is related to a non mongo database:
        SQLAlchemy.create_engine methods parameters.
        base_parameters can be set to a dictionary containing parameters to use when calling SQLAlchemy.declarative_base
     Otherwise (mongo):
        pymongo.MongoClient constructor parameters.
    :return Database object.
     In case database connection URL is related to a non mongo database: SQLAlchemy base instance.
     Otherwise (mongo): pymongo.Database instance.
    :raises NoDatabaseProvided in case no database connection URL is provided.
    :raises NoRelatedControllers in case no controllers are provided.
    """
    if not database_connection_url:
        raise NoDatabaseProvided()
    if not controllers:
        raise NoRelatedControllers()

    if database_connection_url.startswith("mongo"):
        from layabase._database_mongo import _load
    else:
        from layabase._database_sqlalchemy import _load

    return _load(database_connection_url, controllers, **kwargs)
