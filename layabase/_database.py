import enum
import logging
from typing import List, Union
import collections.abc

from layaberr import ValidationFailed
import flask_restplus

from layabase.exceptions import ControllerModelNotSet

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


def load(database_connection_url: str, controllers: collections.abc.Iterable, **kwargs):
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
     In case database connection URL is related to a non mongo database: SQLAlchemy.create_engine methods parameters.
     Otherwise (mongo): pymongo.MongoClient constructor parameters.
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
        import layabase.database_mongo as database_mongo

        return database_mongo._load(database_connection_url, controllers, **kwargs)

    import layabase._database_sqlalchemy as database_sqlalchemy

    return database_sqlalchemy._load(database_connection_url, controllers, **kwargs)


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
        import layabase.database_mongo as database_mongo

        return database_mongo._check(base)
    else:
        import layabase._database_sqlalchemy as database_sqlalchemy

        return database_sqlalchemy._check(base)


def _ignore_read_only_fields(model_properties: dict, model_as_dict: dict):
    if model_as_dict:
        if not isinstance(model_as_dict, dict):
            raise ValidationFailed(model_as_dict, message="Must be a dictionary.")
        read_only_fields = [
            field_name
            for field_name, field_properties in model_properties.items()
            if field_properties.get("readOnly")
        ]
        return {
            field_name: field_value
            for field_name, field_value in model_as_dict.items()
            if field_name not in read_only_fields
        }
    return model_as_dict


class CRUDController:
    """
    Class providing methods to interact with a CRUDModel.
    """

    def __init__(
        self,
        model,
        history=False,
        audit=False,
        interpret_star_character=False,
        skip_name_check=False,
        skip_unknown_fields=True,
        skip_update_indexes=False,
    ):
        """
        Create a new controller to manipulate a table or mongo collection.

        :param model: Naive python class describing requested fields
        :param history: True to be able to rollback to any state in the past. No history by default.
        :param audit: True to keep record of every action on the underlying model. No audit by default.
        :param interpret_star_character: True to consider "*" character in queried field values as %LIKE%. Disabled by default.
        :param skip_name_check: True to be able to force the usage of forbidden table or collection names. Name check is enforced by default.
        :param skip_unknown_fields: False to use strict field name check. Ignore unknown fields by default.
        :param skip_update_indexes: True to never update indexes. Warning, this might lead to invalid indexes on the underlying table or collection.
        """
        if not model:
            raise Exception("Model must be provided.")

        self.model = model
        self.history = history
        self.audit = audit
        self.interpret_star_character = interpret_star_character
        self.skip_name_check = skip_name_check
        self.skip_unknown_fields = skip_unknown_fields
        self.skip_update_indexes = skip_update_indexes

        self._model = None  # Generated from model, appropriate class depending on what was requested on controller

        # CRUD request parsers
        self.query_get_parser = None
        self.query_delete_parser = None
        self.query_rollback_parser = None
        self.query_get_history_parser = None
        self.query_get_audit_parser = None

        # CRUD model definition (instead of request parsers)
        self.json_post_model = None
        self.json_put_model = None

        # CRUD response marshallers
        self.get_response_model = None
        self.get_history_response_model = None
        self.get_audit_response_model = None
        self.get_model_description_response_model = None

        # The response that is always sent for the Model Description
        self._model_description_dictionary = None

    def set_model(self, model_class):
        """
        Initialize related model (should extends (Version)CRUDModel).

        :param model_class: Mongo or SQLAlchemy (Version)CRUDModel.
        """
        self._model = model_class
        if not self._model:
            raise ControllerModelNotSet(self)
        self.query_get_parser = self._model.query_get_parser()
        self.query_delete_parser = self._model.query_delete_parser()
        self.query_rollback_parser = self._model.query_rollback_parser()
        self.query_get_history_parser = self._model.query_get_history_parser()
        self.query_get_audit_parser = (
            self._model.audit_model.query_get_parser()
            if self._model.audit_model
            else None
        )
        self._model_description_dictionary = self._model.description_dictionary()

    def namespace(self, namespace: flask_restplus.Namespace):
        """
        Create Flask RestPlus models that can be used to marshall results (and document service).
        This method should always be called AFTER controller has already been provided as parameter of layabase.load function

        :param namespace: Flask RestPlus API.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        self.json_post_model = namespace.model(
            f"{self.model.__name__}_PostRequestModel",
            self._model.post_fields(namespace),
        )
        self.json_put_model = namespace.model(
            f"{self.model.__name__}_PutRequestModel", self._model.put_fields(namespace)
        )
        self.get_response_model = namespace.model(
            f"{self.model.__name__}_GetResponseModel", self._model.get_fields(namespace)
        )
        self.get_history_response_model = namespace.model(
            f"{self.model.__name__}_GetHistoryResponseModel",
            self._model.history_fields(namespace),
        )
        self.get_audit_response_model = (
            namespace.model(
                f"{self.model.__name__}_GetAuditResponseModel",
                self._model.audit_model.get_fields(namespace),
            )
            if self._model.audit_model
            else None
        )
        self.get_model_description_response_model = namespace.model(
            f"{self.model.__name__}_GetDescriptionResponseModel",
            self._model.description_fields(),
        )

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
        if hasattr(self.json_post_model, "_schema"):
            new_dict = _ignore_read_only_fields(
                self.json_post_model._schema.get("properties", {}), new_dict
            )
        return self._model.add(new_dict)

    def post_many(self, new_dicts: List[dict]) -> List[dict]:
        """
        Add models formatted as a list of dictionaries.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted models formatted as a list of dictionaries.
        """
        if not self._model:
            raise ControllerModelNotSet(self)
        if new_dicts and hasattr(self.json_post_model, "_schema"):
            if not isinstance(new_dicts, list):
                raise ValidationFailed(
                    new_dicts, message="Must be a list of dictionaries."
                )
            new_dicts = [
                _ignore_read_only_fields(
                    self.json_post_model._schema.get("properties", {}), new_dict
                )
                for new_dict in new_dicts
            ]
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
