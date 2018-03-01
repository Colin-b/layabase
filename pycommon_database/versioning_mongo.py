import copy
from typing import List

from pycommon_database.database_mongo import CRUDModel, Column, IndexType
from pycommon_database.flask_restplus_errors import ModelCouldNotBeFound


class VersioningCRUDModel(CRUDModel):

    rev_from = Column(int, should_auto_increment=True, description='Revision starting the validity (inclusive).')
    rev_to = Column(int, is_nullable=True, allow_none_as_filter=True, index_type=IndexType.Unique, description='Revision ending the validity (exclusive).')

    @classmethod
    def _insert_one(cls, model_as_dict: dict) -> dict:
        model_as_dict[cls.rev_to.name] = None
        cls.__collection__.insert_one(model_as_dict)
        return model_as_dict

    @classmethod
    def _insert_many(cls, models_as_list_of_dict: List[dict]):
        for model_as_dict in models_as_list_of_dict:
            model_as_dict[cls.rev_to.name] = None
        cls.__collection__.insert_many(models_as_list_of_dict)

    @classmethod
    def _update_one(cls, model_as_dict: dict) -> (dict, dict):
        model_as_dict_keys = cls._to_primary_keys_model(model_as_dict)
        model_as_dict_keys[cls.rev_to.name] = None
        previous_model_as_dict = cls.__collection__.find_one(model_as_dict_keys)
        if not previous_model_as_dict:
            raise ModelCouldNotBeFound(model_as_dict_keys)

        # Update rev_to
        model_as_dict_keys.pop(cls.rev_to.name)
        cls.__collection__.update_one(model_as_dict_keys, {'$set': {cls.rev_to.name: cls._increment(cls.rev_from.name)}})

        # Insert new row
        current_model_as_dict = copy.deepcopy(previous_model_as_dict)
        model_as_dict = {**current_model_as_dict, **model_as_dict, cls.rev_to.name: None}
        cls.deserialize_insert(model_as_dict)
        cls.__collection__.insert_one(model_as_dict)

        model_as_dict_keys[cls.rev_to.name] = None
        return previous_model_as_dict, cls.__collection__.find_one(model_as_dict_keys)

    @classmethod
    def _delete_many(cls, model_to_query: dict) -> int:
        model_to_query[cls.rev_to.name] = None
        return cls.__collection__.update_many(model_to_query, {'$set': {cls.rev_to.name: cls._increment(cls.rev_from.name)}}).modified_count