import copy
import datetime
from typing import List

from pycommon_database.database_mongo import CRUDModel, Column, IndexType
from pycommon_database.flask_restplus_errors import ModelCouldNotBeFound


class VersioningCRUDModel(CRUDModel):

    valid_since_utc = Column(datetime.datetime, description='Record is valid since this date time (UTC).')
    valid_until_utc = Column(datetime.datetime, is_nullable=True, allow_none_as_filter=True, index_type=IndexType.Unique, description='Record is valid until this date time (UTC).')

    @classmethod
    def _insert_one(cls, model_as_dict: dict) -> dict:
        model_as_dict[cls.valid_since_utc.name] = datetime.datetime.utcnow()
        model_as_dict[cls.valid_until_utc.name] = None
        cls.__collection__.insert_one(model_as_dict)
        return model_as_dict

    @classmethod
    def _insert_many(cls, models_as_list_of_dict: List[dict]):
        now = datetime.datetime.utcnow()
        for model_as_dict in models_as_list_of_dict:
            model_as_dict[cls.valid_since_utc.name] = now
            model_as_dict[cls.valid_until_utc.name] = None
        cls.__collection__.insert_many(models_as_list_of_dict)

    @classmethod
    def _update_one(cls, model_as_dict: dict) -> (dict, dict):
        model_as_dict_keys = cls._to_primary_keys_model(model_as_dict)
        model_as_dict_keys[cls.valid_until_utc.name] = None
        previous_model_as_dict = cls.__collection__.find_one(model_as_dict_keys)
        if not previous_model_as_dict:
            raise ModelCouldNotBeFound(model_as_dict_keys)

        now = datetime.datetime.utcnow()

        # Update rev_to
        model_as_dict_keys.pop(cls.valid_until_utc.name)
        cls.__collection__.update_one(model_as_dict_keys, {'$set': {cls.valid_until_utc.name: now}})

        # Insert new row
        current_model_as_dict = copy.deepcopy(previous_model_as_dict)
        model_as_dict = {**current_model_as_dict, **model_as_dict, cls.valid_since_utc.name: now, cls.valid_until_utc.name: None}
        cls.deserialize_insert(model_as_dict)
        cls.__collection__.insert_one(model_as_dict)

        model_as_dict_keys[cls.valid_until_utc.name] = None
        return previous_model_as_dict, cls.__collection__.find_one(model_as_dict_keys)

    @classmethod
    def _delete_many(cls, model_to_query: dict) -> int:
        model_to_query[cls.valid_until_utc.name] = None
        now = datetime.datetime.utcnow()
        return cls.__collection__.update_many(model_to_query, {'$set': {cls.valid_until_utc.name: now}}).modified_count
