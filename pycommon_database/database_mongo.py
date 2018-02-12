import logging
import datetime
import enum
import os.path
import inspect
import pymongo
import pymongo.errors
from typing import List
from flask_restplus import fields as flask_restplus_fields, reqparse
from bson.objectid import ObjectId

from pycommon_database.flask_restplus_errors import ValidationFailed, ModelCouldNotBeFound
from pycommon_database.audit_mongo import create_from as create_audit_from

logger = logging.getLogger(__name__)


class MongoField:
    def __init__(self, *args, **kwargs):
        self.name = kwargs.pop('name', None)
        self.type_ = kwargs.pop('type', None)
        self.key = kwargs.pop('key', self.name)
        self.primary_key = kwargs.pop('primary_key', False)
        self.nullable = kwargs.pop('nullable', not self.primary_key)
        self.default = kwargs.pop('default', None)
        if self.default is None:
            self.default = [] if self.type_ == list else {} if self.type_ == dict else None
        self.choices = list(self.type_.__members__.keys()) if isinstance(self.type_, enum.EnumMeta) else None
        self.required = kwargs.pop('required', False)
        self.index = kwargs.pop('index', False)
        self.unique = kwargs.pop('unique', False)
        self.doc = kwargs.pop('doc', None)
        self.auto_increment = kwargs.pop('auto_increment', None)


class CRUDModel:
    __db__ = None
    __collection__ = None
    __counters__ = None
    __unique_indexes__ = None
    __indexes__ = None

    @classmethod
    def _validate_input(cls, model_as_dict: dict, mandatory_fields: list):
        # TODO Use an intersection instead?
        for mandatory_field in mandatory_fields:
            if mandatory_field not in model_as_dict:
                raise Exception(f'{mandatory_field} is mandatory.')

        # make sure all enum fields have the correct value available in choices when provided
        enum_fields = {field.name: field.choices for field in cls.get_fields() if field.choices}
        for field in model_as_dict:
            if field in enum_fields and model_as_dict[field] not in enum_fields[field]:
                raise Exception(f'Field {field} was given value {model_as_dict[field]}, not part of allowed list of values {enum_fields[field]}')

    @classmethod
    def create_indexes(cls):
        """
        Create indexes if any.
        """
        cls.__unique_indexes__ = cls._create_indexes(True)
        cls.__indexes__ = cls._create_indexes(False)

    @classmethod
    def _create_indexes(cls, unique: bool):
        try:
            criteria = [(field.name, pymongo.ASCENDING) for field in cls.get_fields() if
                        field.index and field.unique == unique]
            if criteria:
                logger.debug(
                    f"Create a unique?({unique}) index on {cls.__collection__.name} using {criteria} criteria.")
                return cls.__collection__.create_index(criteria, unique=unique)
            return None
        except pymongo.errors.DuplicateKeyError:
            logger.exception(f'Duplicate key found for {criteria} criteria when creating a unique index.')
            raise

    @classmethod
    def get_all(cls, **kwargs):
        """
        Return all models formatted as a list of dictionaries.
        """
        query = cls._build_query(**kwargs)
        all_docs = cls.__collection__.find(query)
        return list(all_docs)

    @classmethod
    def add_all(cls, models_as_list_of_dict: list):
        """
        Add models formatted as a list of dictionaries.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted model formatted as a list of dictionaries.
        """
        if not models_as_list_of_dict:
            raise ValidationFailed({}, message='No data provided.')
        mandatory_fields = [field.name for field in cls.get_fields() if not field.nullable and not field.auto_increment]
        for model_dict in models_as_list_of_dict:
            cls._validate_input(model_dict, mandatory_fields)
            # if _id present in a document, convert it to ObjectId
            if '_id' in model_dict.keys():
                model_dict['_id'] = ObjectId(model_dict['_id'])
            # handle auto-incrementation of fields when needed
            for auto_inc_field in [field for field in cls.get_fields() if field.auto_increment]:
                model_dict[auto_inc_field.name] = cls._increment(auto_inc_field)

        cls.__collection__.insert(models_as_list_of_dict)
        return models_as_list_of_dict

    @classmethod
    def _increment(cls, field: MongoField):
        if field.auto_increment == 'inc' and cls.__counters__:
            counter_key = {'_id': cls.__collection__.name}
            counter_element = cls.__counters__.find_one(counter_key)
            if not counter_element:
                # counter not created yet, create it with default value 1
                cls.__counters__.insert({'_id': cls.__collection__.name, field.name: 1})
                counter_element = cls.__counters__.find_one(counter_key)
                # TODO Missing return instruction ?
            elif field.name not in counter_element.keys():
                cls.__counters__.update(counter_key, {'$set': {field.name: 1}})
            else:
                cls.__counters__.update(counter_key, {'$inc': {field.name: 1}})
            return cls.__counters__.find_one(counter_key)[field.name]
        return 1

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
        cls._validate_input(model_as_dict, mandatory_fields=[])
        # if _id present in a document, convert it to ObjectId
        if '_id' in model_as_dict.keys():
            model_as_dict['_id'] = ObjectId(model_as_dict['_id'])
        model_as_dict_keys = cls._to_primary_keys_model(model_as_dict)
        previous_model_as_dict = cls.__collection__.find_one(model_as_dict_keys)
        if not previous_model_as_dict:
            raise ModelCouldNotBeFound(model_as_dict)

        model_as_dict_updates = {k: v for k, v in model_as_dict.items() if k not in model_as_dict_keys}
        raw_result = cls.__collection__.update_one(model_as_dict_keys, {'$set': model_as_dict_updates}).raw_result
        new_model_as_dict = cls.__collection__.find_one(model_as_dict_keys)
        return previous_model_as_dict, new_model_as_dict

    @classmethod
    def _to_primary_keys_model(cls, model_as_dict: dict) -> dict:
        primary_key_fields = [field for field in cls.get_fields() if field.primary_key]
        # TODO Avoid iteration here, be more pythonic
        for primary_key in primary_key_fields:
            if primary_key not in model_as_dict:
                raise Exception(f'{primary_key} is mandatory.')
        return {k: v for k, v in model_as_dict.items() if k in primary_key_fields}

    @classmethod
    def remove(cls, **kwargs):
        """
        Remove the model(s) matching those criterion.
        :returns Number of removed rows.
        """
        if not kwargs:
            raise Exception('Remove called without criteria. At least one criterion is mandatory,')
        query = cls._build_query(**kwargs)
        nb_removed = cls.__collection__.delete_many(query).deleted_count
        return nb_removed

    @staticmethod
    def _build_query(**kwargs) -> dict:
        return {key: ObjectId(value) if key == '_id' else value for key, value in kwargs.items() if value is not None}

    @classmethod
    def query_get_parser(cls):
        return cls._query_parser()

    @classmethod
    def query_delete_parser(cls):
        return cls._query_parser()

    @classmethod
    def _query_parser(cls):
        query_parser = reqparse.RequestParser()
        for field in cls.get_fields():
            if field.type_ == list:
                query_parser.add_argument(
                    field.name,
                    required=False,
                    type=str,
                    action='append'
                )
            else:
                query_parser.add_argument(
                    field.name,
                    required=False,
                    type=field.type_
                )
        return query_parser

    @classmethod
    def description_dictionary(cls) -> dict:
        description = {
            'collection': cls.__collection__.full_name,
        }
        for field in cls.get_fields():
            description[field.key] = field.name
        return description

    @classmethod
    def flask_restplus_fields(cls) -> dict:
        return {
            field.name: _get_flask_restplus_type(field)(
                required=field.required,
                example=_get_example(field),
                description=field.doc,
                enum=field.choices,
                default=field.default,
                readonly=field.auto_increment is not None,
                cls_or_instance=_get_rest_plus_subtype(field)
            )
            for field in cls.get_fields()
        }

    @classmethod
    def flask_restplus_description_fields(cls) -> dict:
        exported_fields = {
            'collection': flask_restplus_fields.String(required=True, example='collection', description='Collection name'),
        }

        exported_fields.update({
            field.name: flask_restplus_fields.String(
                required=field.required,
                example='column',
                description=field.doc,
            )
            for field in cls.get_fields()
        })
        return exported_fields

    @classmethod
    def get_fields(cls) -> List[MongoField]:
        """
        :return: list of all Mongo fields (can be empty)
        """
        return [attribute[1] for attribute in inspect.getmembers(cls) if type(attribute[1]) == MongoField]

    @classmethod
    def create_audit(cls):
        return create_audit_from(cls)


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

    logger.info(f'Connecting to {database_connection_url}...')
    if database_connection_url.startswith('mongomock'):
        import mongomock  # This is a test dependency only
        client = mongomock.MongoClient()
    else:
        database_name = os.path.basename(database_connection_url)
        client = pymongo.MongoClient(database_connection_url)[database_name]
    logger.debug(f'Creating models...')
    for model_class in create_models_func(client):
        model_class.create_indexes()
    return client


def reset(client):
    """
    If the database was already created, then drop all tables and recreate them all.
    """
    # TODO Reset should be able to retrieve models from client
    if client and model_classes:
        for model_class in model_classes:
            _reset_collection(client, model_class)
        logger.info(f'All data related to {client.metadata.bind.url} reset.')


class NoDatabaseProvided(Exception):
    def __init__(self):
        Exception.__init__(self, 'A database connection URL must be provided.')


class NoRelatedModels(Exception):
    def __init__(self):
        Exception.__init__(self, 'A method allowing to create related models must be provided.')


def _reset_collection(client, model_class):
    logger.info(f'Resetting all data related to collection "{model_class.__collection__.name}"...')
    nb_removed = model_class.__collection__.delete_many({}).deleted_count
    logger.info(f'{nb_removed} records deleted')

    logger.info(f'Drop collection "{model_class.__collection__.name}"')
    model_class.__collection__.drop()

    logger.info(f'Resetting counter "{model_class.__collection__.name}" located in collection "counters"')
    nb_removed = client['counters'].delete_many({'_id': model_class.__collection__.name}).deleted_count
    logger.info(f'{nb_removed} records deleted')


def _get_flask_restplus_type(field: MongoField):
    """
    Return the Flask RestPlus field type (as a class) corresponding to this Mongo field.

    :raises Exception if field type is not managed yet.
    """
    if field.type_ == str:
        return flask_restplus_fields.String
    if field.type_ == int:
        return flask_restplus_fields.Integer
    if field.type_ == float:
        return flask_restplus_fields.Float
    if field.type_ == bool:
        return flask_restplus_fields.Boolean
    if field.type_ == datetime.date:
        return flask_restplus_fields.Date
    if field.type_ == datetime.datetime:
        return flask_restplus_fields.DateTime
    if isinstance(field.type_,enum.EnumMeta):
        return flask_restplus_fields.String
    if field.type_ == list:
        return flask_restplus_fields.List
    if field.type_ == dict:
        return flask_restplus_fields.Raw

    raise Exception(f'Flask RestPlus field type cannot be guessed for {field} field.')


def _get_rest_plus_subtype(field: MongoField):
    """
    Return the Flask RestPlus field subtype (as a class) corresponding to this Mongo field if it is a list, else same as type

    :raises Exception if field type is not managed yet.
    """
    if field.type_ == list:
        return flask_restplus_fields.List(cls_or_instance=flask_restplus_fields.String)
    return _get_flask_restplus_type(field)


def _get_example(field: MongoField) -> str:
    if field.default:
        return str(field.default)

    return str(field.choices[0]) if field.choices else _get_default_example(field)


def _get_default_example(field: MongoField) -> str:
    """
    Return an Example value corresponding to this Mongodb field.
    """
    field_flask = _get_flask_restplus_type(field)
    if field_flask == flask_restplus_fields.Integer:
        return '0'
    if field_flask == flask_restplus_fields.Float:
        return '0.0'
    if field_flask == flask_restplus_fields.Boolean:
        return 'true'
    if field_flask == flask_restplus_fields.Date:
        return '2017-09-24'
    if field_flask == flask_restplus_fields.DateTime:
        return '2017-09-24T15:36:09'
    if field_flask == flask_restplus_fields.List:
        return str([['field1','value1'], ['fieldx','valuex']])
    if field_flask == flask_restplus_fields.Raw:
        return str({'field1':'value1','fieldx':'valuex'})
    return 'sample_value'
