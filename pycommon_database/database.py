import logging
from typing import List

logger = logging.getLogger(__name__)


def _is_mongo(database_connection_url: str) -> bool:
    return database_connection_url.startswith('mongo')


class NoDatabaseProvided(Exception):
    def __init__(self):
        Exception.__init__(self, 'A database connection URL must be provided.')


class NoRelatedModels(Exception):
    def __init__(self):
        Exception.__init__(self, 'A method allowing to create related models must be provided.')


class NoDumpDataProvided(Exception):
    def __init__(self):
        Exception.__init__(self, 'Dump Data must be provided.')


def load(database_connection_url: str, create_models_func: callable, **kwargs):
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
    :param create_models_func: Function that will be called to create models and return them (instances of CRUDModel)
     (Mandatory).
    :param kwargs: Additional custom parameters:
     for SQLAlchemy: create_engine methods parameters.
     for Mongo: unused.
    """
    if not database_connection_url:
        raise NoDatabaseProvided()
    if not create_models_func:
        raise NoRelatedModels()

    if _is_mongo(database_connection_url):
        import pycommon_database.database_mongo as database_mongo
        return database_mongo._load(database_connection_url, create_models_func)

    import pycommon_database.database_sqlalchemy as database_sqlalchemy
    return database_sqlalchemy._load(database_connection_url, create_models_func, **kwargs)


def reset(base):
    """
    If the database was already created, then drop all tables and recreate them all.
    """
    if hasattr(base, 'is_mongos'):
        import pycommon_database.database_mongo as database_mongo
        database_mongo._reset(base)
    else:
        import pycommon_database.database_sqlalchemy as database_sqlalchemy
        database_sqlalchemy._reset(base)


def list_content(base) -> List[str]:
    """
    List all the collections part of the provided database

    :param base: database object as returned by the _load method (Mandatory).
    :returns The list of all collections contained in the database
     TODO not supported yet for non Mongo DBs
    """
    if not base:
        raise NoDatabaseProvided()

    if hasattr(base, 'is_mongos'):
        import pycommon_database.database_mongo as database_mongo
        return database_mongo._list_content(base)


def dump(base, collection: str) -> str:
    """
    Dump the content of the provided collection part of the provided database in a bson string

    :param base: database object as returned by the load function (Mandatory).
    :param collection: name of the collection to dump (Mandatory).
     TODO not supported yet for non Mongo DBs
    :returns The database dump formatted as a bson string '<bson_content>'.
    """
    if not base:
        raise NoDatabaseProvided()

    if hasattr(base, 'is_mongos'):
        import pycommon_database.database_mongo as database_mongo
        return database_mongo._dump(base, collection)


def restore(base, dump_content: List[dict]):
    """
    Restore in the provided database the provided content in the provided collection name.

    :param base: database object as returned by the load function (Mandatory).
    :param dump_content: dictionary holding the name of the colections to restore as keys and the collection dump formatted as a bson dump as values (Mandatory)
     TODO not supported yet for non Mongo DBs
    """
    if not base:
        raise NoDatabaseProvided()
    if not dump_content:
        raise NoDumpDataProvided()

    if hasattr(base, 'is_mongos'):
        import pycommon_database.database_mongo as database_mongo
        database_mongo._restore(base, dump_content)


class ControllerModelNotSet(Exception):
    def __init__(self, controller_class):
        Exception.__init__(self,
                           f'Model was not attached to {controller_class.__name__}. Call {controller_class.model}.')


def _ignore_read_only_fields(model_properties: dict, model_as_dict: dict):
    read_only_fields = [field_name for field_name, field_properties in model_properties.items() if field_properties.get('readOnly')]
    if model_as_dict:
        return {field_name: field_value for field_name, field_value in model_as_dict.items() if field_name not in read_only_fields}
    return model_as_dict


class CRUDController:
    """
    Class providing methods to interact with a CRUDModel.
    """
    _model = None

    # CRUD request parsers
    query_get_parser = None
    query_delete_parser = None
    query_rollback_parser = None
    query_get_history_parser = None
    query_get_audit_parser = None

    # CRUD model definition (instead of request parsers)
    json_post_model = None
    json_put_model = None

    # CRUD response marshallers
    get_response_model = None
    get_history_response_model = None
    get_audit_response_model = None
    get_model_description_response_model = None

    # The response that is always sent for the Model Description
    _model_description_dictionary = None

    @classmethod
    def model(cls, model_class):
        """
        Initialize related model (should extends (Version)CRUDModel).

        :param model_class: Mongo or SQLAlchemy (Version)CRUDModel.
        """
        cls._model = model_class
        if not cls._model:
            raise ControllerModelNotSet(cls)
        cls.query_get_parser = cls._model.query_get_parser()
        cls.query_delete_parser = cls._model.query_delete_parser()
        cls.query_rollback_parser = cls._model.query_rollback_parser()
        cls.query_get_history_parser = cls._model.query_get_history_parser()
        cls.query_get_audit_parser = cls._model.audit_model.query_get_parser() if cls._model.audit_model else None
        cls._model_description_dictionary = cls._model.description_dictionary()

    @classmethod
    def namespace(cls, namespace):
        """
        Create Flask RestPlus models that can be used to marshall results (and document service).
        This method should always be called AFTER cls.model()

        :param namespace: Flask RestPlus API.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        cls.json_post_model = cls._model.json_post_model(namespace)
        cls.json_put_model = cls._model.json_put_model(namespace)
        cls.get_response_model = cls._model.get_response_model(namespace)
        cls.get_history_response_model = cls._model.get_history_response_model(namespace)
        cls.get_audit_response_model = cls._model.audit_model.get_response_model(namespace) if cls._model.audit_model else None
        cls.get_model_description_response_model = namespace.model(''.join([cls._model.__name__, 'Description']),
                                                                   cls._model.flask_restplus_description_fields())

    @classmethod
    def get(cls, request_arguments: dict) -> List[dict]:
        """
        Return all models formatted as a list of dictionaries.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        return cls._model.get_all(**request_arguments)

    @classmethod
    def get_one(cls, request_arguments: dict) -> dict:
        """
        Return a model formatted as a dictionary.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        return cls._model.get(**request_arguments)

    @classmethod
    def post(cls, new_dict: dict) -> dict:
        """
        Add a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted model formatted as a dictionary.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        if hasattr(cls.json_post_model, '_schema'):
            new_dict = _ignore_read_only_fields(cls.json_post_model._schema.get('properties', {}), new_dict)
        return cls._model.add(new_dict)

    @classmethod
    def post_many(cls, new_dicts: List[dict]) -> List[dict]:
        """
        Add models formatted as a list of dictionaries.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted models formatted as a list of dictionaries.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        if new_dicts and hasattr(cls.json_post_model, '_schema'):
            new_dicts = [
                _ignore_read_only_fields(cls.json_post_model._schema.get('properties', {}), new_dict)
                for new_dict in new_dicts
            ]
        return cls._model.add_all(new_dicts)

    @classmethod
    def put(cls, updated_dict: dict) -> (dict, dict):
        """
        Update a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns A tuple containing previous model formatted as a dictionary (first item)
        and new model formatted as a dictionary (second item).
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        return cls._model.update(updated_dict)

    @classmethod
    def put_many(cls, updated_dicts: List[dict]) -> (List[dict], List[dict]):
        """
        Update models formatted as a list of dictionaries.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns A tuple containing previous models formatted as a list of dictionaries (first item)
        and new models formatted as a list of dictionaries (second item).
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        return cls._model.update_all(updated_dicts)

    @classmethod
    def delete(cls, request_arguments: dict) -> int:
        """
        Remove the model(s) matching those criterion.
        :returns Number of removed rows.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        return cls._model.remove(**request_arguments)

    @classmethod
    def get_audit(cls, request_arguments: dict) -> List[dict]:
        """
        Return all audit models formatted as a list of dictionaries.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        if not cls._model.audit_model:
            return []
        return cls._model.audit_model.get_all(**request_arguments)

    @classmethod
    def get_model_description(cls) -> dict:
        if not cls._model_description_dictionary:
            raise ControllerModelNotSet(cls)
        return cls._model_description_dictionary

    @classmethod
    def rollback_to(cls, request_arguments: dict) -> int:
        """
        Rollback to the model(s) matching those criterion.
        :returns Number of affected rows.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        return cls._model.rollback_to(**request_arguments)

    @classmethod
    def get_history(cls, request_arguments: dict) -> List[dict]:
        """
        Return all models formatted as a list of dictionaries.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        return cls._model.get_history(**request_arguments)

    @classmethod
    def get_field_names(cls) -> List[str]:
        """
        Return all model field names formatted as a str list.
        """
        if not cls._model:
            raise ControllerModelNotSet(cls)
        return cls._model.get_field_names()
