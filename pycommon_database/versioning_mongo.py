from flask_restplus import inputs
import pymongo
from typing import List

from pycommon_database.database_mongo import CRUDModel, Column, IndexType
from pycommon_database.flask_restplus_errors import ValidationFailed, ModelCouldNotBeFound


REVISION_COUNTER = ('revision', 'shared')


class VersionedCRUDModel(CRUDModel):
    """
    CRUDModel with ability to retrieve history and rollback to a previous version of a row.
    It is mandatory for at least one field to be a unique index.
    """

    valid_since_revision = Column(int, description='Record is valid since this revision (included).')
    valid_until_revision = Column(int, allow_none_as_filter=True, index_type=IndexType.Unique,
                                  description='Record is valid until this revision (excluded).')

    @classmethod
    def json_post_model(cls, namespace):
        all_fields = cls._flask_restplus_fields(namespace)
        del all_fields[cls.valid_since_revision.name]
        del all_fields[cls.valid_until_revision.name]
        return namespace.model(f'{cls.__name__}_Versioned', all_fields)

    @classmethod
    def _insert_one(cls, model_as_dict: dict) -> dict:
        revision = cls._increment(*REVISION_COUNTER)
        model_as_dict[cls.valid_since_revision.name] = revision
        model_as_dict[cls.valid_until_revision.name] = None
        cls.__collection__.insert_one(model_as_dict)
        if cls.audit_model:
            cls.audit_model.audit_add(revision)
        return model_as_dict

    @classmethod
    def _insert_many(cls, models_as_list_of_dict: List[dict]):
        revision = cls._increment(*REVISION_COUNTER)
        for model_as_dict in models_as_list_of_dict:
            model_as_dict[cls.valid_since_revision.name] = revision
            model_as_dict[cls.valid_until_revision.name] = None
        cls.__collection__.insert_many(models_as_list_of_dict)
        if cls.audit_model:
            cls.audit_model.audit_add(revision)

    @classmethod
    def json_put_model(cls, namespace):
        all_fields = cls._flask_restplus_fields(namespace)
        del all_fields[cls.valid_since_revision.name]
        del all_fields[cls.valid_until_revision.name]
        return namespace.model(f'{cls.__name__}_Versioned', all_fields)

    @classmethod
    def _update_one(cls, model_as_dict: dict) -> (dict, dict):
        model_as_dict_keys = cls._to_primary_keys_model(model_as_dict)
        model_as_dict_keys[cls.valid_until_revision.name] = None
        previous_model_as_dict = cls.__collection__.find_one(model_as_dict_keys, projection={'_id': False})
        if not previous_model_as_dict:
            raise ModelCouldNotBeFound(model_as_dict_keys)

        revision = cls._increment(*REVISION_COUNTER)

        # Set previous version as expired (insert previous as expired)
        cls.__collection__.insert_one({**previous_model_as_dict, cls.valid_until_revision.name: revision})

        # Update valid version (update previous)
        model_as_dict[cls.valid_since_revision.name] = revision
        model_as_dict[cls.valid_until_revision.name] = None
        new_model_as_dict = cls.__collection__.find_one_and_update(model_as_dict_keys, {'$set': model_as_dict},
                                                                   return_document=pymongo.ReturnDocument.AFTER)
        if cls.audit_model:
            cls.audit_model.audit_update(revision)
        return previous_model_as_dict, new_model_as_dict

    @classmethod
    def query_delete_parser(cls):
        query_delete_parser = super().query_delete_parser()
        query_delete_parser.remove_argument(cls.valid_since_revision.name)
        query_delete_parser.remove_argument(cls.valid_until_revision.name)
        return query_delete_parser

    @classmethod
    def remove(cls, **model_to_query) -> int:
        model_to_query.pop(cls.valid_since_revision.name, None)
        model_to_query[cls.valid_until_revision.name] = None
        return super().remove(**model_to_query)

    @classmethod
    def _delete_many(cls, model_to_query: dict) -> int:
        revision = cls._increment(*REVISION_COUNTER)
        if cls.audit_model:
            cls.audit_model.audit_remove(revision)
        return cls.__collection__.update_many(model_to_query, {'$set': {cls.valid_until_revision.name: revision}}).modified_count

    @classmethod
    def query_rollback_parser(cls):
        query_rollback_parser = cls._query_parser()
        query_rollback_parser.remove_argument(cls.valid_since_revision.name)
        query_rollback_parser.remove_argument(cls.valid_until_revision.name)
        query_rollback_parser.add_argument('revision', type=inputs.positive, required=True)
        return query_rollback_parser

    @classmethod
    def _get_revision(cls, model_to_query: dict) -> int:
        revision = model_to_query.get('revision')
        if not revision:
            raise ValidationFailed(model_to_query, {'revision': ['Missing data for required field.']})

        if not isinstance(revision, int):
            raise ValidationFailed(model_to_query, {'revision': [f'Not a valid int.']})

        del model_to_query['revision']
        return revision

    @classmethod
    def query_get_parser(cls):
        query_get_parser = super().query_get_parser()
        query_get_parser.remove_argument(cls.valid_since_revision.name)
        query_get_parser.remove_argument(cls.valid_until_revision.name)
        return query_get_parser

    @classmethod
    def get_response_model(cls, namespace):
        all_fields = cls._flask_restplus_fields(namespace)
        del all_fields[cls.valid_since_revision.name]
        del all_fields[cls.valid_until_revision.name]
        return namespace.model(f'{cls.__name__}_Versioned', all_fields)

    @classmethod
    def get(cls, **model_to_query) -> dict:
        """
        Return valid model corresponding to query.
        """
        model_to_query.pop(cls.valid_since_revision.name, None)
        model_to_query[cls.valid_until_revision.name] = None
        return super().get(**model_to_query)

    @classmethod
    def get_all(cls, **model_to_query) -> List[dict]:
        """
        Return all valid models corresponding to query.
        """
        model_to_query.pop(cls.valid_since_revision.name, None)
        model_to_query[cls.valid_until_revision.name] = None
        return super().get_all(**model_to_query)

    @classmethod
    def get_history(cls, **model_to_query) -> List[dict]:
        """
        Return all models corresponding to query.
        """
        return super().get_all(**model_to_query)

    @classmethod
    def rollback_to(cls, **model_to_query) -> int:
        revision = cls._get_revision(model_to_query)

        errors = cls.validate_query(model_to_query)
        if errors:
            raise ValidationFailed(model_to_query, errors)

        cls.deserialize_query(model_to_query)

        # Select those who were valid at the time of the revision
        previously_expired = {
            cls.valid_since_revision.name: {'$lte': revision},
            cls.valid_until_revision.name: {'$exists': True, '$ne': None, '$gt': revision},
        }
        previously_expired_models = cls.__collection__.find({**model_to_query, **previously_expired},
                                                            projection={'_id': False})
        previously_expired_models = list(previously_expired_models)  # Convert Cursor to list

        new_revision = cls._increment(*REVISION_COUNTER)

        # Update currently valid as non valid anymore (new version since this validity)
        for expired_model in previously_expired_models:
            expired_model_keys = cls._to_primary_keys_model(expired_model)
            expired_model_keys[cls.valid_until_revision.name] = None

            actual_model_as_dict = cls.__collection__.find_one(expired_model_keys)
            if actual_model_as_dict:
                cls.__collection__.update_many(expired_model_keys, {'$set': {cls.valid_until_revision.name: new_revision}})

        # Update currently valid as non valid anymore (they were not existing at the time)
        new_still_valid = {
            cls.valid_since_revision.name: {'$gt': revision},
            cls.valid_until_revision.name: None,
        }
        nb_removed = cls.__collection__.update_many({**model_to_query, **new_still_valid},
                                                    {'$set': {cls.valid_until_revision.name: new_revision}}).modified_count

        # Insert expired as valid
        for expired_model in previously_expired_models:
            expired_model[cls.valid_since_revision.name] = new_revision
            expired_model[cls.valid_until_revision.name] = None

        if previously_expired_models:
            cls.__collection__.insert_many(previously_expired_models)

        if cls.audit_model:
            cls.audit_model.audit_rollback(new_revision)
        return len(previously_expired_models) + nb_removed
