import copy
import datetime
import dateutil.parser
from flask_restplus import inputs
from typing import List

from pycommon_database.database_mongo import CRUDModel, Column, IndexType
from pycommon_database.flask_restplus_errors import ValidationFailed, ModelCouldNotBeFound


class VersionedCRUDModel(CRUDModel):

    valid_since_utc = Column(datetime.datetime, description='Record is valid since this date time (UTC).')
    valid_until_utc = Column(datetime.datetime, is_nullable=True, allow_none_as_filter=True,
                             index_type=IndexType.Unique, description='Record is valid until this date time (UTC).')

    @classmethod
    def json_post_model(cls, namespace):
        all_fields = cls._flask_restplus_fields(namespace)
        del all_fields[cls.valid_since_utc.name]
        del all_fields[cls.valid_until_utc.name]
        return namespace.model(f'{cls.__name__}_Versioned', all_fields)

    @classmethod
    def json_put_model(cls, namespace):
        all_fields = cls._flask_restplus_fields(namespace)
        del all_fields[cls.valid_since_utc.name]
        del all_fields[cls.valid_until_utc.name]
        return namespace.model(f'{cls.__name__}_Versioned', all_fields)

    @classmethod
    def _insert_one(cls, model_as_dict: dict) -> dict:
        model_as_dict[cls.valid_since_utc.name] = datetime.datetime.utcnow()
        model_as_dict[cls.valid_until_utc.name] = None
        cls.__collection__.insert_one(model_as_dict)
        if cls.audit_model:
            cls.audit_model.audit_add(model_as_dict)
        return model_as_dict

    @classmethod
    def _insert_many(cls, models_as_list_of_dict: List[dict]):
        now = datetime.datetime.utcnow()
        for model_as_dict in models_as_list_of_dict:
            model_as_dict[cls.valid_since_utc.name] = now
            model_as_dict[cls.valid_until_utc.name] = None
        cls.__collection__.insert_many(models_as_list_of_dict)
        if cls.audit_model:
            for inserted_dict in models_as_list_of_dict:
                cls.audit_model.audit_add(inserted_dict)

    @classmethod
    def _update_one(cls, model_as_dict: dict) -> (dict, dict):
        model_as_dict_keys = cls._to_primary_keys_model(model_as_dict)
        model_as_dict_keys[cls.valid_until_utc.name] = None
        previous_model_as_dict = cls.__collection__.find_one(model_as_dict_keys)
        if not previous_model_as_dict:
            raise ModelCouldNotBeFound(model_as_dict_keys)

        now = datetime.datetime.utcnow()

        # Update rev_to
        cls.__collection__.update_one(model_as_dict_keys, {'$set': {cls.valid_until_utc.name: now}})

        # Insert new row
        current_model_as_dict = copy.deepcopy(previous_model_as_dict)
        model_as_dict = {**current_model_as_dict, **model_as_dict, cls.valid_since_utc.name: now,
                         cls.valid_until_utc.name: None}
        cls.deserialize_insert(model_as_dict, should_increment=False)  # TODO Only to remove dot notation
        cls.__collection__.insert_one(model_as_dict)

        new_model_as_dict = cls.__collection__.find_one(model_as_dict_keys)
        if cls.audit_model:
            cls.audit_model.audit_update(new_model_as_dict)
        return previous_model_as_dict, new_model_as_dict

    @classmethod
    def _delete_many(cls, model_to_query: dict) -> int:
        if cls.audit_model:
            cls.audit_model.audit_remove(**model_to_query)
        model_to_query[cls.valid_until_utc.name] = None
        now = datetime.datetime.utcnow()
        return cls.__collection__.update_many(model_to_query, {'$set': {cls.valid_until_utc.name: now}}).modified_count

    @classmethod
    def query_rollback_parser(cls):
        query_rollback_parser = cls._query_parser()
        query_rollback_parser.remove_argument(cls.valid_since_utc.name)
        query_rollback_parser.remove_argument(cls.valid_until_utc.name)
        query_rollback_parser.add_argument('validity', type=inputs.datetime_from_iso8601, required=True)
        return query_rollback_parser

    @classmethod
    def _get_validity(cls, model_to_query: dict) -> datetime.datetime:
        validity = model_to_query.get('validity')
        if not validity:
            raise ValidationFailed(model_to_query, {'validity': ['Missing data for required field.']})

        if isinstance(validity, str):
            try:
                validity = dateutil.parser.parse(validity)
            except ValueError or OverflowError:
                raise ValidationFailed(model_to_query, {'validity': ['Not a valid datetime.']})

        if not isinstance(validity, datetime.datetime):
            raise ValidationFailed(model_to_query, {'validity': [f'Not a valid datetime.']})

        del model_to_query['validity']
        return validity

    @classmethod
    def rollback_to(cls, **model_to_query) -> int:
        validity = cls._get_validity(model_to_query)

        errors = cls.validate_query(model_to_query)
        if errors:
            raise ValidationFailed(model_to_query, errors)

        cls.deserialize_query(model_to_query)

        previously_expired = {
            cls.valid_since_utc.name: {'$lte': validity},
            cls.valid_until_utc.name: {'$gt': validity},
        }
        previously_expired_models = cls.__collection__.find({**model_to_query, **previously_expired},
                                                            projection={'_id': False})
        previously_expired_models = list(previously_expired_models)  # Convert Cursor to list

        now = datetime.datetime.utcnow()

        # Update currently valid as non valid anymore (new version since this validity)
        for expired_model in previously_expired_models:
            expired_model_keys = cls._to_primary_keys_model(expired_model)
            expired_model_keys[cls.valid_until_utc.name] = None

            actual_model_as_dict = cls.__collection__.find_one(expired_model_keys)
            if actual_model_as_dict:
                expired_model_keys.pop(cls.valid_until_utc.name)
                cls.__collection__.update_many(expired_model_keys, {'$set': {cls.valid_until_utc.name: now}})

        # Update currently valid as non valid anymore (they were not existing at the time)
        new_still_valid = {
            cls.valid_since_utc.name: {'$gt': validity},
            cls.valid_until_utc.name: None,
        }
        nb_removed = cls.__collection__.update_many({**model_to_query, **new_still_valid},
                                                    {'$set': {cls.valid_until_utc.name: now}}).modified_count

        # Insert expired as valid
        for expired_model in previously_expired_models:
            expired_model[cls.valid_since_utc.name] = now
            expired_model[cls.valid_until_utc.name] = None

        if previously_expired_models:
            cls.__collection__.insert_many(previously_expired_models)

        return len(previously_expired_models) + nb_removed
