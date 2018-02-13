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

logger = logging.getLogger(__name__)


class Column:
    """
    Definition of a Mondo Database field.
    """

    UNIQUE_INDEX = 'unique'
    NON_UNIQUE_INDEX = 'non_unique'

    def __init__(self, field_type=str, **kwargs):
        """

        :param field_type: Python field type. Default to str.

        :param default_value: Default value matching type. Default to None.
        :param description: Field description.
        :param index_type: Type of index amongst UNIQUE_INDEX or NON_UNIQUE_INDEX. Default to None.
        :param is_primary_key: bool value. Default to False.
        :param is_nullable: bool value. Default to opposite of is_primary_key (True)
        :param is_required: bool value. Default to False.
        :param should_auto_increment: bool value. Default to False. Only valid for int fields.
        """
        self.name = None
        self.field_type = field_type or str
        self.choices = list(self.field_type.__members__.keys()) if isinstance(self.field_type, enum.EnumMeta) else None
        self.default_value = kwargs.pop('default_value', None)
        if self.default_value is None:
            self.default_value = [] if self.field_type == list else {} if self.field_type == dict else None
        self.description = kwargs.pop('description', None)
        self.index_type = kwargs.pop('index_type', None)

        self.is_primary_key = bool(kwargs.pop('is_primary_key', False))
        self.is_nullable = bool(kwargs.pop('is_nullable', not self.is_primary_key))
        # TODO This field should be validated as well if it is only for client input
        self.is_required = bool(kwargs.pop('is_required', False))
        self.should_auto_increment = bool(kwargs.pop('should_auto_increment', False))
        if self.should_auto_increment and self.field_type is not int:
            raise Exception('Only int fields can be auto incremented.')

    def __str__(self):
        return f'{self.name}'


def to_mongo_field(attribute):
    attribute[1].name = attribute[0]
    return attribute[1]


class CRUDModel:
    __tablename__ = None  # Name of the collection described by this model
    __collection__ = None
    __counters__ = None

    @classmethod
    def _base(cls, base):
        cls.__collection__ = base[cls.__tablename__]
        cls.__counters__ = base['counters']
        cls._create_indexes(Column.UNIQUE_INDEX)
        cls._create_indexes(Column.NON_UNIQUE_INDEX)

    @classmethod
    def _create_indexes(cls, index_type: str):
        try:
            criteria = [(field.name, pymongo.ASCENDING) for field in cls.get_fields() if field.index_type == index_type]
            if criteria:
                # Avoid using auto generated index name that might be too long
                index_name = f'uidx{cls.__collection__.name}' if index_type == Column.UNIQUE_INDEX else f'idx{cls.__collection__.name}'
                logger.debug(f"Create {index_name} {index_type} index on {cls.__collection__.name} using {criteria} criteria.")
                cls.__collection__.create_index(criteria, unique=index_type == Column.UNIQUE_INDEX, name=index_name)
        except pymongo.errors.DuplicateKeyError:
            logger.exception(f'Duplicate key found for {criteria} criteria when creating a {index_type} index.')
            raise

    @classmethod
    def get_all(cls, **kwargs) -> list:
        """
        Return all models formatted as a list of dictionaries.
        """
        query = cls._build_query(**kwargs)
        return [model for model in cls.__collection__.find(query) if model.pop('_id')]

    @classmethod
    def add_all(cls, models_as_list_of_dict: list) -> list:
        """
        Add models formatted as a list of dictionaries.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted models formatted as a list of dictionaries.
        """
        if not models_as_list_of_dict:
            raise ValidationFailed({}, message='No data provided.')
        mandatory_fields = [field.name for field in cls.get_fields() if
                            not field.is_nullable and not field.should_auto_increment]
        for model_as_dict in models_as_list_of_dict:
            cls._validate_input(model_as_dict, mandatory_fields)
            # if _id present in a document, convert it to ObjectId
            if '_id' in model_as_dict.keys():
                model_as_dict['_id'] = ObjectId(model_as_dict['_id'])
            # handle auto-incrementation of fields when needed
            for auto_inc_field in [field for field in cls.get_fields() if field.should_auto_increment]:
                model_as_dict[auto_inc_field.name] = cls._increment(auto_inc_field)

        cls.__collection__.insert(models_as_list_of_dict)
        return [model for model in models_as_list_of_dict if model.pop('_id')]

    @classmethod
    def add(cls, model_as_dict: dict) -> dict:
        """
        Add a model formatted as a dictionary.
        :raises ValidationFailed in case Marshmallow validation fail.
        :returns The inserted model formatted as a dictionary.
        """
        if not model_as_dict:
            raise ValidationFailed({}, message='No data provided.')
        mandatory_fields = [field.name for field in cls.get_fields() if
                            not field.is_nullable and not field.should_auto_increment]
        cls._validate_input(model_as_dict, mandatory_fields)
        # if _id present in a document, convert it to ObjectId
        if '_id' in model_as_dict.keys():
            model_as_dict['_id'] = ObjectId(model_as_dict['_id'])
        # handle auto-incrementation of fields when needed
        for auto_inc_field in [field for field in cls.get_fields() if field.should_auto_increment]:
            model_as_dict[auto_inc_field.name] = cls._increment(auto_inc_field)

        cls.__collection__.insert(model_as_dict)
        del model_as_dict['_id']
        return model_as_dict

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
                raise Exception(
                    f'"{field}" value "{model_as_dict[field]}" should be amongst {enum_fields[field]}.')

    @classmethod
    def _increment(cls, field: Column):
        counter_key = {'_id': cls.__collection__.name}
        counter_element = cls.__counters__.find_one(counter_key)
        if not counter_element:
            # counter not created yet, create it with default value 1
            cls.__counters__.insert({'_id': cls.__collection__.name, field.name: 1})
        elif field.name not in counter_element.keys():
            cls.__counters__.update(counter_key, {'$set': {field.name: 1}})
        else:
            cls.__counters__.update(counter_key, {'$inc': {field.name: 1}})
        return cls.__counters__.find_one(counter_key)[field.name]

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
        del previous_model_as_dict['_id']
        del new_model_as_dict['_id']
        return previous_model_as_dict, new_model_as_dict

    @classmethod
    def _to_primary_keys_model(cls, model_as_dict: dict) -> dict:
        primary_key_fields = [field.name for field in cls.get_fields() if field.is_primary_key]
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
            if field.field_type == list:
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
                    type=field.field_type
                )
        return query_parser

    @classmethod
    def description_dictionary(cls) -> dict:
        description = {
            'collection': cls.__tablename__,
        }
        for field in cls.get_fields():
            description[field.name] = field.name
        return description

    @classmethod
    def flask_restplus_fields(cls) -> dict:
        return {
            field.name: _get_flask_restplus_type(field)(
                required=field.is_required,
                example=_get_example(field),
                description=field.description,
                enum=field.choices,
                default=field.default_value,
                readonly=field.should_auto_increment,
                cls_or_instance=_get_rest_plus_subtype(field)
            )
            for field in cls.get_fields()
        }

    @classmethod
    def flask_restplus_description_fields(cls) -> dict:
        exported_fields = {
            'collection': flask_restplus_fields.String(required=True, example='collection',
                                                       description='Collection name'),
        }

        exported_fields.update({
            field.name: flask_restplus_fields.String(
                required=field.is_required,
                example='column',
                description=field.description,
            )
            for field in cls.get_fields()
        })
        return exported_fields

    @classmethod
    def get_fields(cls) -> List[Column]:
        """
        :return: list of all Mongo fields (can be empty)
        """
        return [to_mongo_field(attribute) for attribute in inspect.getmembers(cls) if type(attribute[1]) == Column]

    @classmethod
    def create_audit(cls):
        from pycommon_database.audit_mongo import create_from
        return create_from(cls)


def load(database_connection_url: str, create_models_func: callable):
    """
    Create all necessary tables and perform the link between models and underlying database connection.

    :param database_connection_url: URL formatted as a standard database connection string (Mandatory).
    :param create_models_func: Function that will be called to create models and return them (instances of CRUDModel)
    (Mandatory).
    """
    if not database_connection_url:
        raise NoDatabaseProvided()
    if not create_models_func:
        raise NoRelatedModels()

    logger.info(f'Connecting to {database_connection_url}...')
    database_name = os.path.basename(database_connection_url)
    if database_connection_url.startswith('mongomock'):
        import mongomock  # This is a test dependency only
        client = mongomock.MongoClient()
    else:
        client = pymongo.MongoClient(database_connection_url)
    base = client[database_name]
    logger.debug(f'Creating models...')
    for model_class in create_models_func(base):
        model_class._base(base)
    return base


def reset(base):
    """
    If the database was already created, then drop all tables and recreate them all.
    """
    if base:
        for collection in base._collections.values():
            _reset_collection(base, collection)


class NoDatabaseProvided(Exception):
    def __init__(self):
        Exception.__init__(self, 'A database connection URL must be provided.')


class NoRelatedModels(Exception):
    def __init__(self):
        Exception.__init__(self, 'A method allowing to create related models must be provided.')


def _reset_collection(base, collection):
    logger.info(f'Resetting all data related to "{collection.name}" collection...')
    nb_removed = collection.delete_many({}).deleted_count
    logger.info(f'{nb_removed} records deleted.')

    logger.info(f'Drop collection "{collection.name}".')
    collection.drop()

    logger.info(f'Resetting counters."{collection.name}".')
    nb_removed = base['counters'].delete_many({'_id': collection.name}).deleted_count
    logger.info(f'{nb_removed} counter records deleted')


def _get_flask_restplus_type(field: Column):
    """
    Return the Flask RestPlus field type (as a class) corresponding to this Mongo field.

    :raises Exception if field type is not managed yet.
    """
    if field.field_type == str:
        return flask_restplus_fields.String
    if field.field_type == int:
        return flask_restplus_fields.Integer
    if field.field_type == float:
        return flask_restplus_fields.Float
    if field.field_type == bool:
        return flask_restplus_fields.Boolean
    if field.field_type == datetime.date:
        return flask_restplus_fields.Date
    if field.field_type == datetime.datetime:
        return flask_restplus_fields.DateTime
    if isinstance(field.field_type, enum.EnumMeta):
        return flask_restplus_fields.String
    if field.field_type == list:
        return flask_restplus_fields.List
    if field.field_type == dict:
        return flask_restplus_fields.Raw

    raise Exception(f'Flask RestPlus field type cannot be guessed for {field} field.')


def _get_rest_plus_subtype(field: Column):
    """
    Return the Flask RestPlus field subtype (as a class) corresponding to this Mongo field if it is a list,
    else same as type

    :raises Exception if field type is not managed yet.
    """
    if field.field_type == list:
        return flask_restplus_fields.List(cls_or_instance=flask_restplus_fields.String)
    return _get_flask_restplus_type(field)


def _get_example(field: Column) -> str:
    if field.default_value:
        return str(field.default_value)

    return str(field.choices[0]) if field.choices else _get_default_example(field)


def _get_default_example(field: Column) -> str:
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
