from bson.objectid import ObjectId
import pymongo
import enum
import inspect
import logging

logger = logging.getLogger(__name__)

class MongoColumn:
    def __init__(self, *args, **kwargs):
        self.name = kwargs.pop('name', None)
        self.type_ = kwargs.pop('type', None)
        self.key = kwargs.pop('key', self.name)
        self.primary_key = kwargs.pop('primary_key', False)
        self.nullable = kwargs.pop('nullable', not self.primary_key)
        self.default = kwargs.pop('default', None)
        self.required = kwargs.pop('required', False)
        self.index = kwargs.pop('index', False)
        self.unique = kwargs.pop('unique', False)
        self.doc = kwargs.pop('doc', None)
        self.autoincrement = kwargs.pop('autoincrement', None)


def mongo_inspect(mongo_class):
    mapper = list(column for column in inspect.getmembers(mongo_class) if type(column[1]) == MongoColumn)
    return mapper

def mongo_create_indexes(mongo_class, unique=True):
    try:
        columns = get_mongo_field_values(mongo_class)
        criteria = [(column.name, pymongo.ASCENDING) for column in columns if column.index and column.unique == unique]
        if criteria != []:
            print(f"create an index on {mongo_class.__collection__.name} using criterias {criteria}, unique = {unique}")
            result = mongo_class.__collection__.create_index(criteria, unique = unique)
            return result
        return None
    except pymongo.errors.DuplicateKeyError:
        logger.exception(f'duplicate key found for criteria {criteria} when creating a unique index')
        raise
    except:
        raise

def mongo_build_query(**kwargs):
    query = {}
    for key, value in kwargs.items():
        if value is not None:
            if key == '_id':
                query[key] = ObjectId(value)
            else:
                query[key] = value
    return query

def mongo_get_choices(field):
    if isinstance(field.type_, enum.EnumMeta):
        return list(field.type_.__members__.keys())
    return None

def get_mongo_field_values(mongo_class):
    return list(field[1] for field in inspect.getmembers(mongo_class) if type(field[1]) == MongoColumn)

def get_mongo_enum_field_values(mongo_class):
    enum_fields = {}
    for field in inspect.getmembers(mongo_class):
        if type(field[1]) == MongoColumn:
            enum_values = mongo_get_choices(field[1])
            if enum_values:
                enum_fields[field[1].name] = enum_values
    return enum_fields

def mongo_get_pimary_keys_list(mongo_class):
    mongo_fields = inspect.getmembers(mongo_class)
    return list(field[0] for field in mongo_fields if type(field[1]) == MongoColumn and field[1].primary_key)

def mongo_get_primary_keys_values(mongoclass, model_as_dict, check_valid_key=True):
    primary_key_fields = mongo_get_pimary_keys_list(mongoclass)
    model_as_dict_keys = {k: v for k, v in model_as_dict.items() if k in primary_key_fields}
    if check_valid_key:
        for primary_key in primary_key_fields:
            if primary_key not in model_as_dict_keys.keys():
                logger.exception(f'{primary_key} should be specified as it is a primary key')
                raise Exception(f'{primary_key} should be specified as it is a primary key')
    return model_as_dict_keys

def get_mongo_autoincrement_field_values(mongo_class):
    return list(field[1] for field in inspect.getmembers(mongo_class) if type(field[1]) == MongoColumn and field[1].autoincrement)

def get_mongo_non_nullable_fields(mongo_class):
    return list(field[1].name for field in inspect.getmembers(mongo_class)
                if type(field[1]) == MongoColumn and not field[1].nullable and not field[1].autoincrement)

def mongo_validate_fields(mongo_class, models_as_list_of_dict, check_nullable=True):
    non_nullable_fields = (get_mongo_non_nullable_fields(mongo_class) if check_nullable else [])
    enum_fields = get_mongo_enum_field_values(mongo_class)
    if not isinstance(models_as_list_of_dict, (list, tuple)):
        models_as_list_of_dict = [models_as_list_of_dict]
    for model_dict in models_as_list_of_dict:
        """ make sure all non nullable fields have been provided """
        for non_nullable_field in non_nullable_fields:
            if non_nullable_field not in model_dict.keys():
                logger.exception(f'All non nullable fields {non_nullable_fields} have to be provided.')
                raise Exception(f'All non nullable fields {non_nullable_fields} have to be provided.')
        """ make sure all enum fields have the correct value available in choices when provided """
        for field in model_dict.keys():
            if field in enum_fields and model_dict[field] not in enum_fields[field]:
                logger.exception(f'Field {field} was given value {model_dict[field]}, not part of allowed list of values {enum_fields[field]}')
                raise Exception(f'Field {field} was given value {model_dict[field]}, not part of allowed list of values {enum_fields[field]}')

def mongo_get_list_fields(mongo_class):
    mongo_class_fields = get_mongo_field_values(mongo_class)
    return [mongo_class_field.name for mongo_class_field in mongo_class_fields if mongo_class_field.type_ == list]
