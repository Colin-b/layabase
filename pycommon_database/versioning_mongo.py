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
    def _insert_one(cls, document: dict) -> dict:
        revision = cls._increment(*REVISION_COUNTER)
        document[cls.valid_since_revision.name] = revision
        document[cls.valid_until_revision.name] = None
        cls.__collection__.insert_one(document)
        if cls.audit_model:
            cls.audit_model.audit_add(revision)
        return document

    @classmethod
    def _insert_many(cls, documents: List[dict]):
        revision = cls._increment(*REVISION_COUNTER)
        for document in documents:
            document[cls.valid_since_revision.name] = revision
            document[cls.valid_until_revision.name] = None
        cls.__collection__.insert_many(documents)
        if cls.audit_model:
            cls.audit_model.audit_add(revision)

    @classmethod
    def json_put_model(cls, namespace):
        all_fields = cls._flask_restplus_fields(namespace)
        del all_fields[cls.valid_since_revision.name]
        del all_fields[cls.valid_until_revision.name]
        return namespace.model(f'{cls.__name__}_Versioned', all_fields)

    @classmethod
    def _update_one(cls, document: dict) -> (dict, dict):
        document_keys = cls._to_primary_keys_model(document)
        document_keys[cls.valid_until_revision.name] = None
        previous_document = cls.__collection__.find_one(document_keys, projection={'_id': False})
        if not previous_document:
            raise ModelCouldNotBeFound(document_keys)

        revision = cls._increment(*REVISION_COUNTER)

        # Set previous version as expired (insert previous as expired)
        cls.__collection__.insert_one({**previous_document, cls.valid_until_revision.name: revision})

        # Update valid version (update previous)
        document[cls.valid_since_revision.name] = revision
        document[cls.valid_until_revision.name] = None
        new_document = cls.__collection__.find_one_and_update(document_keys, {'$set': document},
                                                              return_document=pymongo.ReturnDocument.AFTER)
        if cls.audit_model:
            cls.audit_model.audit_update(revision)
        return previous_document, new_document

    @classmethod
    def query_delete_parser(cls):
        query_delete_parser = super().query_delete_parser()
        query_delete_parser.remove_argument(cls.valid_since_revision.name)
        query_delete_parser.remove_argument(cls.valid_until_revision.name)
        return query_delete_parser

    @classmethod
    def remove(cls, **filters) -> int:
        filters.pop(cls.valid_since_revision.name, None)
        filters[cls.valid_until_revision.name] = None
        return super().remove(**filters)

    @classmethod
    def _delete_many(cls, filters: dict) -> int:
        revision = cls._increment(*REVISION_COUNTER)
        if cls.audit_model:
            cls.audit_model.audit_remove(revision)
        return cls.__collection__.update_many(filters, {'$set': {cls.valid_until_revision.name: revision}}).modified_count

    @classmethod
    def query_rollback_parser(cls):
        query_rollback_parser = cls._query_parser()
        query_rollback_parser.remove_argument(cls.valid_since_revision.name)
        query_rollback_parser.remove_argument(cls.valid_until_revision.name)
        query_rollback_parser.add_argument('revision', type=inputs.positive, required=True)
        return query_rollback_parser

    @classmethod
    def _get_revision(cls, filters: dict) -> int:
        # TODO Use an int Column validate + deserializa
        revision = filters.get('revision')
        if not revision:
            raise ValidationFailed(filters, {'revision': ['Missing data for required field.']})

        if not isinstance(revision, int):
            raise ValidationFailed(filters, {'revision': [f'Not a valid int.']})

        del filters['revision']
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
    def get(cls, **filters) -> dict:
        """
        Return valid document corresponding to query.
        """
        filters.pop(cls.valid_since_revision.name, None)
        filters[cls.valid_until_revision.name] = None
        return super().get(**filters)

    @classmethod
    def get_all(cls, **filters) -> List[dict]:
        """
        Return all valid documents corresponding to query.
        """
        filters.pop(cls.valid_since_revision.name, None)
        filters[cls.valid_until_revision.name] = None
        return super().get_all(**filters)

    @classmethod
    def get_history(cls, **filters) -> List[dict]:
        return super().get_all(**filters)

    @classmethod
    def rollback_to(cls, **filters) -> int:
        revision = cls._get_revision(filters)

        errors = cls.validate_query(filters)
        if errors:
            raise ValidationFailed(filters, errors)

        cls.deserialize_query(filters)

        # Select those who were valid at the time of the revision
        previously_expired = {
            cls.valid_since_revision.name: {'$lte': revision},
            cls.valid_until_revision.name: {'$exists': True, '$ne': None, '$gt': revision},
        }
        expired_documents = cls.__collection__.find({**filters, **previously_expired}, projection={'_id': False})
        expired_documents = list(expired_documents)  # Convert Cursor to list

        new_revision = cls._increment(*REVISION_COUNTER)

        # Update currently valid as non valid anymore (new version since this validity)
        for expired_document in expired_documents:
            expired_document_keys = cls._to_primary_keys_model(expired_document)
            expired_document_keys[cls.valid_until_revision.name] = None

            cls.__collection__.find_one_and_update(expired_document_keys,
                                                   {'$set': {cls.valid_until_revision.name: new_revision}})

        # Update currently valid as non valid anymore (they were not existing at the time)
        new_still_valid = {
            cls.valid_since_revision.name: {'$gt': revision},
            cls.valid_until_revision.name: None,
        }
        nb_removed = cls.__collection__.update_many({**filters, **new_still_valid},
                                                    {'$set': {cls.valid_until_revision.name: new_revision}}).modified_count

        # Insert expired as valid
        for expired_document in expired_documents:
            expired_document[cls.valid_since_revision.name] = new_revision
            expired_document[cls.valid_until_revision.name] = None

        if expired_documents:
            cls.__collection__.insert_many(expired_documents)

        if cls.audit_model:
            cls.audit_model.audit_rollback(new_revision)
        return len(expired_documents) + nb_removed
