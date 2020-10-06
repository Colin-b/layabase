import copy
import datetime
import inspect
import logging
import os.path
from typing import List, Dict, Union, Type, Iterable

import pymongo
import pymongo.errors
import pymongo.database

from layabase import CRUDController
from layabase.mongo import Column, DictColumn, IndexType, link
from layabase._exceptions import ValidationFailed

logger = logging.getLogger(__name__)


_server_versions: Dict[str, str] = {}


class _CRUDModel:
    """
    Class providing CRUD helper methods for a Mongo model.
    __collection__ class property must be specified in Model.
    __counters__ class property must be specified in Model.
    Calling load_from(...) will provide you those properties.
    """

    __collection_name__: str = None  # Name of the collection described by this model
    __collection__: pymongo.collection.Collection = None  # Mongo collection
    __counters__: pymongo.collection.Collection = None  # Mongo counters collection (to increment fields)
    __fields__: List[Column] = []  # All Mongo fields within this model
    audit_model: Type["_CRUDModel"] = None
    _skip_unknown_fields: bool = True
    _skip_log_for_unknown_fields: List[str] = []
    logger = None
    _server_version: str = ""

    def __init_subclass__(cls, base: pymongo.database.Database = None, **kwargs):
        cls._skip_unknown_fields = kwargs.pop("skip_unknown_fields", True)
        cls._skip_log_for_unknown_fields = kwargs.pop("skip_log_for_unknown_fields", [])
        skip_name_check = kwargs.pop("skip_name_check", False)
        skip_update_indexes = kwargs.pop("skip_update_indexes", False)
        super().__init_subclass__(**kwargs)
        cls.logger = logging.getLogger(f"{__name__}.{cls.__collection_name__}")
        cls.__fields__ = [
            field
            for field_name, field in inspect.getmembers(cls)
            if isinstance(field, Column)
        ]
        # TODO Remove the need for this check, only create models with a base
        if base is not None:  # Allow to not provide base to create fake models
            if not skip_name_check and cls._is_forbidden():
                raise Exception(
                    f"{cls.__collection_name__} is a reserved collection name."
                )
            cls.__collection__ = base[cls.__collection_name__]
            cls.__counters__ = base["counters"]
            cls._server_version = _server_versions.get(base.name, "")
            if not skip_update_indexes:
                cls.update_indexes()

    @classmethod
    def get_primary_keys(cls) -> List[str]:
        return [field.name for field in cls.__fields__ if field.is_primary_key]

    @classmethod
    def _is_forbidden(cls):
        # Counters collection is managed by layabase
        # Audit collections are managed by layabase
        return (
            not cls.__collection_name__
            or "counters" == cls.__collection_name__
            or cls.__collection_name__.startswith("audit")
        )

    @classmethod
    def update_indexes(cls, document: dict = None):
        """
        Drop all indexes and recreate them.
        As advised in https://docs.mongodb.com/manual/tutorial/manage-indexes/#modify-an-index
        """
        if cls._check_indexes(document):
            cls.logger.info("Updating indexes...")
            cls.__collection__.drop_indexes()
            cls._create_indexes(IndexType.Unique, document)
            cls._create_indexes(IndexType.Other, document)
            cls.logger.info("Indexes updated.")
            if cls.audit_model:
                cls.audit_model.update_indexes(document)

    @classmethod
    def _check_indexes(cls, document: dict) -> bool:
        """
        Check if indexes are present and if criteria have been modified
        :param document: Data specified by the user at the time of the index creation.
        """
        criteria = [
            field_name
            for field_name in cls._get_index_fields(IndexType.Other, document, "")
        ]
        unique_criteria = [
            field_name
            for field_name in cls._get_index_fields(IndexType.Unique, document, "")
        ]
        index_name = f"idx{cls.__collection_name__}"
        unique_index_name = f"uidx{cls.__collection_name__}"
        indexes = cls.__collection__.list_indexes()
        cls.logger.debug(f"Checking existing indexes: {indexes}")
        indexes = {
            index["name"]: index["key"].keys()
            for index in indexes
            if "name" in index and "key" in index
        }
        return (
            (criteria and index_name not in indexes)
            or (not criteria and index_name in indexes)
            or (criteria and index_name in indexes and criteria != indexes[index_name])
            or (unique_criteria and unique_index_name not in indexes)
            or (not unique_criteria and unique_index_name in indexes)
            or (
                unique_criteria
                and unique_index_name in indexes
                and unique_criteria != indexes[unique_index_name]
            )
        )

    @classmethod
    def _create_indexes(cls, index_type: IndexType, document: dict, condition=None):
        """
        Create indexes of specified type.
        :param document: Data specified by the user at the time of the index creation.
        """
        try:
            criteria = [
                (field_name, pymongo.ASCENDING)
                for field_name in cls._get_index_fields(index_type, document, "")
            ]
            if criteria:
                # Avoid using auto generated index name that might be too long
                index_name = (
                    f"uidx{cls.__collection_name__}"
                    if index_type == IndexType.Unique
                    else f"idx{cls.__collection_name__}"
                )
                cls.logger.info(
                    f"Create {index_name} {index_type.name} index on {cls.__collection_name__} using {criteria} criteria."
                )
                if condition is None or cls._server_version < "3.2":
                    cls.__collection__.create_index(
                        criteria, unique=index_type == IndexType.Unique, name=index_name
                    )
                else:
                    try:
                        cls.__collection__.create_index(
                            criteria,
                            unique=index_type == IndexType.Unique,
                            name=index_name,
                            partialFilterExpression=condition,
                        )
                    except pymongo.errors.OperationFailure:
                        cls.logger.exception(
                            f"Unable to create a {index_type.name} index."
                        )
                        cls.__collection__.create_index(
                            criteria,
                            unique=index_type == IndexType.Unique,
                            name=index_name,
                        )
        except pymongo.errors.DuplicateKeyError:
            cls.logger.exception(
                f"Duplicate key found for {criteria} criteria "
                f"when creating a {index_type.name} index."
            )
            raise

    @classmethod
    def _get_index_fields(
        cls, index_type: IndexType, document: Union[dict, None], prefix: str
    ) -> List[str]:
        """
        In case a field is a dictionary and some fields within it should be indexed, override this method.
        """
        index_fields = [
            f"{prefix}{field.name}"
            for field in cls.__fields__
            if field.index_type == index_type
        ]
        for field in cls.__fields__:
            if isinstance(field, DictColumn):
                index_fields.extend(
                    field._get_index_fields(index_type, document, prefix)
                )
        return index_fields

    @classmethod
    def get(cls, **filters) -> dict:
        """
        Return the document matching provided filters.
        """
        errors = cls.validate_query(filters)
        if errors:
            raise ValidationFailed(filters, errors)

        cls.deserialize_query(filters)

        if cls.__collection__.count_documents(filters) > 1:
            raise ValidationFailed(
                filters, message="More than one result: Consider another filtering."
            )

        if cls.logger.isEnabledFor(logging.DEBUG):
            cls.logger.debug(f"Query document matching {filters}...")
        document = cls.__collection__.find_one(filters)
        if cls.logger.isEnabledFor(logging.DEBUG):
            cls.logger.debug(
                f'{"1" if document else "No corresponding"} document retrieved.'
            )
        return cls.serialize(document)

    @classmethod
    def get_last(cls, **filters) -> dict:
        """
        Return last revision of the document matching provided filters.
        """
        return cls.get(**filters)

    @classmethod
    def get_all(cls, **filters) -> List[dict]:
        """
        Return all documents matching provided filters.
        """
        limit = filters.pop("limit", 0) or 0
        offset = filters.pop("offset", 0) or 0
        errors = cls.validate_query(filters)
        if errors:
            raise ValidationFailed(filters, errors)

        cls.deserialize_query(filters)

        if cls.logger.isEnabledFor(logging.DEBUG):
            if filters:
                cls.logger.debug(f"Query documents matching {filters}...")
            else:
                cls.logger.debug(f"Query all documents...")
        documents = cls.__collection__.find(filters, skip=offset, limit=limit)
        if cls.logger.isEnabledFor(logging.DEBUG):
            nb_documents = (
                cls.__collection__.count_documents(filters, skip=offset, limit=limit)
                if limit
                else cls.__collection__.count_documents(filters, skip=offset)
            )
            cls.logger.debug(
                f'{nb_documents if nb_documents else "No corresponding"} documents retrieved.'
            )
        return [cls.serialize(document) for document in documents]

    @classmethod
    def get_history(cls, **filters) -> List[dict]:
        """
        Return all documents matching filters.
        """
        return cls.get_all(**filters)

    @classmethod
    def rollback_to(cls, **filters) -> int:
        """
        All records matching the query and valid at specified validity will be considered as valid.
        :return Number of records updated.
        """
        return 0

    @classmethod
    def get_field_names(cls) -> List[str]:
        return [field.name for field in cls.__fields__]

    @classmethod
    def validate_query(cls, filters: dict) -> dict:
        """
        Validate a get or delete request.

        :param filters: Provided filters.
        Each entry if composed of a field name associated to a value.
        :return: Validation errors that might have occurred. Empty if no error occurred.
        Each entry if composed of a field name associated to a list of error messages.
        """
        queried_fields = [
            field.name for field in cls.__fields__ if field.name in filters
        ]
        unknown_fields = [
            field_name for field_name in filters if field_name not in queried_fields
        ]
        known_filters = copy.deepcopy(filters)
        for unknown_field in unknown_fields:
            known_field, field_value = cls._to_known_field(
                unknown_field, known_filters[unknown_field]
            )
            if known_field:
                known_filters.setdefault(known_field.name, {}).update(field_value)

        errors = {}

        for field in [field for field in cls.__fields__ if field.name in known_filters]:
            errors.update(field.validate_query(known_filters))

        return errors

    @classmethod
    def deserialize_query(cls, filters: dict):
        """
        Update values within provided filters to values that can be queried in Mongo.
        Remove entries for unknown fields.

        :param filters: Provided filters.
        Each entry if composed of a field name associated to a value.
        """
        queried_fields = [
            field.name for field in cls.__fields__ if field.name in filters
        ]
        unknown_fields = [
            field_name for field_name in filters if field_name not in queried_fields
        ]
        known_fields = {}  # Contains converted known dot notation fields

        for unknown_field in unknown_fields:
            known_field, field_value = cls._to_known_field(
                unknown_field, filters[unknown_field]
            )
            del filters[unknown_field]
            if known_field:
                known_fields.setdefault(known_field.name, {}).update(field_value)
            elif unknown_field not in cls._skip_log_for_unknown_fields:
                cls.logger.warning(f"Skipping unknown field {unknown_field}.")

        # Deserialize dot notation values
        for field in [field for field in cls.__fields__ if field.name in known_fields]:
            field.deserialize_query(known_fields)
            # Put back deserialized values as dot notation fields
            for inner_field_name, value in known_fields[field.name].items():
                filters[f"{field.name}.{inner_field_name}"] = value

        for field in [field for field in cls.__fields__ if field.name in filters]:
            field.deserialize_query(filters)

    @classmethod
    def _to_known_field(cls, field_name: str, value) -> (Column, dict):
        """
        Convert a dot notation field and its value to a known field and its dictionary value.
        eg:
            field_name = "dict_field.first_key_field"
            value = 3

        Return will be:
            (dict_field_column, {'first_key_field': 3})

        :param field_name: Field name including dot notation. Such as "dict_field.first_key_field".
        :return: Tuple containing dictionary field (first item) and dictionary containing the sub field and its value.
        (None, None) if not found.
        """
        field_names = field_name.split(".", maxsplit=1)
        if len(field_names) == 2:
            for field in cls.__fields__:
                if field.name == field_names[0] and field.field_type == dict:
                    return field, {field_names[1]: value}
        return None, None

    @classmethod
    def serialize(cls, document: dict) -> dict:
        if not document:
            return {}

        for field in cls.__fields__:
            field.serialize(document)

        # Make sure fields that were stored in a previous version of a model are not returned if removed since then
        # It also ensure _id can be skipped unless specified otherwise in the model
        known_fields = [field.name for field in cls.__fields__]
        removed_fields = [
            field_name for field_name in document if field_name not in known_fields
        ]
        if removed_fields:
            for removed_field in removed_fields:
                del document[removed_field]
            # Do not log the fact that _id is removed as it is a Mongo specific field
            if "_id" in removed_fields:
                removed_fields.remove("_id")
            if removed_fields:
                cls.logger.debug(f"Skipping removed fields {removed_fields}.")

        return document

    @classmethod
    def add(cls, document: dict) -> dict:
        """
        Add a model formatted as a dictionary.

        :raises ValidationFailed in case validation fail.
        :returns The inserted model formatted as a dictionary.
        """
        errors = cls.validate_insert(document)
        if errors:
            raise ValidationFailed(document, errors)

        cls.deserialize_insert(document)
        try:
            if cls.logger.isEnabledFor(logging.DEBUG):
                cls.logger.debug(f"Inserting {document}...")
            cls._insert_one(document)
            if cls.logger.isEnabledFor(logging.DEBUG):
                cls.logger.debug("Document inserted.")
            return cls.serialize(document)
        except pymongo.errors.DuplicateKeyError:
            raise ValidationFailed(
                cls.serialize(document), message="This document already exists."
            )

    @classmethod
    def add_all(cls, documents: List[dict]) -> List[dict]:
        """
        Add documents formatted as a list of dictionaries.

        :raises ValidationFailed in case validation fail.
        :returns The inserted documents formatted as a list of dictionaries.
        """
        if not documents:
            raise ValidationFailed([], message="No data provided.")

        if not isinstance(documents, list):
            raise ValidationFailed(documents, message="Must be a list of dictionaries.")

        new_documents = copy.deepcopy(documents)

        errors = cls.validate_and_deserialize_insert(new_documents)
        if errors:
            raise ValidationFailed(documents, errors)

        try:
            if cls.logger.isEnabledFor(logging.DEBUG):
                cls.logger.debug(f"Inserting {new_documents}...")
            cls._insert_many(new_documents)
            if cls.logger.isEnabledFor(logging.DEBUG):
                cls.logger.debug("Documents inserted.")
            return [cls.serialize(document) for document in new_documents]
        except pymongo.errors.BulkWriteError as e:
            raise ValidationFailed(documents, message=str(e.details))

    @classmethod
    def validate_and_deserialize_insert(cls, documents: List[dict]) -> dict:
        errors = {}

        for index, document in enumerate(documents):
            document_errors = cls.validate_insert(document)
            if document_errors:
                errors[index] = document_errors
                continue

            if (
                not errors
            ):  # Skip deserialization in case errors were found as it will stop
                cls.deserialize_insert(document)

        return errors

    @classmethod
    def validate_insert(cls, document: dict) -> dict:
        """
        Validate a document insertion request.

        :param document: Mongo to be document.
        Each entry if composed of a field name associated to a value.
        :return: Validation errors that might have occurred. Empty if no error occurred.
        Entry would be composed of a field name associated to a list of error messages.
        """
        if document is None:
            return {"": ["No data provided."]}

        if not isinstance(document, dict):
            return {"": ["Must be a dictionary."]}

        new_document = copy.deepcopy(document)

        errors = {}

        field_names = [field.name for field in cls.__fields__]
        unknown_fields = [
            field_name for field_name in new_document if field_name not in field_names
        ]
        for unknown_field in unknown_fields:
            known_field, field_value = cls._to_known_field(
                unknown_field, new_document[unknown_field]
            )
            if known_field:
                new_document.setdefault(known_field.name, {}).update(field_value)
            elif not cls._skip_unknown_fields:
                errors.update({unknown_field: ["Unknown field"]})

        for field in cls.__fields__:
            errors.update(field.validate_insert(new_document))

        return errors

    @classmethod
    def _remove_dot_notation(cls, document: dict):
        """
        Update document so that it does not contains dot notation fields.
        Remove entries for unknown fields.
        """
        field_names = [field.name for field in cls.__fields__]
        unknown_fields = [
            field_name for field_name in document if field_name not in field_names
        ]
        for unknown_field in unknown_fields:
            known_field, field_value = cls._to_known_field(
                unknown_field, document[unknown_field]
            )
            del document[unknown_field]
            if known_field:
                document.setdefault(known_field.name, {}).update(field_value)
            elif unknown_field not in cls._skip_log_for_unknown_fields:
                cls.logger.warning(f"Skipping unknown field {unknown_field}.")

    @classmethod
    def deserialize_insert(cls, document: dict):
        """
        Update this document values to values that can be inserted in Mongo.
        Remove entries for unknown fields.
        Convert dot notation fields to corresponding dictionary as dot notation is not allowed on insert.

        :param document: Document that should be inserted.
        Each entry if composed of a field name associated to a value.
        """
        cls._remove_dot_notation(document)

        for field in cls.__fields__:
            field.deserialize_insert(document)
            if field.should_auto_increment:
                document[field.name] = cls._increment(*field.get_counter(document))

    @classmethod
    def _increment(cls, counter_name: str, counter_category: str = None) -> int:
        """
        Increment a counter by one.

        :param counter_name: Name of the counter to increment. Will be created at 0 if not existing yet.
        :param counter_category: Category storing those counters. Default to model table name.
        :return: New counter value.
        """
        counter_key = {
            "_id": counter_category if counter_category else cls.__collection__.name
        }
        counter_update = {
            "$inc": {f"{counter_name}.counter": 1},
            "$set": {f"{counter_name}.last_update_time": datetime.datetime.utcnow()},
        }
        counter_element = cls.__counters__.find_one_and_update(
            counter_key,
            counter_update,
            return_document=pymongo.ReturnDocument.AFTER,
            upsert=True,
        )
        return counter_element[counter_name]["counter"]

    @classmethod
    def _get_counter(cls, counter_name: str, counter_category: str = None) -> int:
        """
        Get current counter value.

        :param counter_name: Name of the counter to retrieve.
        :param counter_category: Category storing those counters. Default to model table name.
        :return: Counter value or 0 if not existing.
        """
        counter_key = {
            "_id": counter_category if counter_category else cls.__collection__.name
        }
        counter_element = cls.__counters__.find_one(counter_key)
        return counter_element[counter_name]["counter"] if counter_element else 0

    @classmethod
    def reset_counters(cls):
        """
        reset the class related counters

        """
        for field in cls.__fields__:
            if field.should_auto_increment:
                cls._reset_counter(*field.get_counter({}))

    @classmethod
    def _reset_counter(cls, counter_name: str):
        """
        Reset a counter.

        :param counter_name: Name of the counter to reset. Will be created at 0 if not existing yet.
        """
        counter_key = {"_id": cls.__collection__.name}
        counter_update = {
            "$set": {
                f"{counter_name}.counter": 0,
                f"{counter_name}.last_update_time": datetime.datetime.utcnow(),
            }
        }
        cls.__counters__.find_one_and_update(counter_key, counter_update, upsert=True)
        return

    @classmethod
    def update(cls, document: dict) -> (dict, dict):
        """
        Update a model formatted as a dictionary.

        :raises ValidationFailed in case validation fail.
        :returns A tuple containing previous document (first item) and new document (second item).
        """
        errors = cls.validate_update(document)
        if errors:
            raise ValidationFailed(document, errors)

        cls.deserialize_update(document)

        try:
            if cls.logger.isEnabledFor(logging.DEBUG):
                cls.logger.debug(f"Updating {document}...")
            previous_document, new_document = cls._update_one(document)
            if cls.logger.isEnabledFor(logging.DEBUG):
                cls.logger.debug(f"Document updated to {new_document}.")
            return cls.serialize(previous_document), cls.serialize(new_document)
        except pymongo.errors.DuplicateKeyError:
            raise ValidationFailed(
                cls.serialize(document), message="This document already exists."
            )

    @classmethod
    def update_all(cls, documents: List[dict]) -> (List[dict], List[dict]):
        """
        Update documents formatted as a list of dictionary.

        :raises ValidationFailed in case validation fail.
        :returns A tuple containing previous documents (first item) and new documents (second item).
        """
        if not documents:
            raise ValidationFailed([], message="No data provided.")

        if not isinstance(documents, list):
            raise ValidationFailed(documents, message="Must be a list.")

        new_documents = copy.deepcopy(documents)

        errors = cls.validate_and_deserialize_update(new_documents)
        if errors:
            raise ValidationFailed(documents, errors)

        try:
            if cls.logger.isEnabledFor(logging.DEBUG):
                cls.logger.debug(f"Updating {new_documents}...")
            previous_documents, updated_documents = cls._update_many(new_documents)
            if cls.logger.isEnabledFor(logging.DEBUG):
                cls.logger.debug(f"Documents updated to {updated_documents}.")
            return (
                [cls.serialize(document) for document in previous_documents],
                [cls.serialize(document) for document in updated_documents],
            )
        except pymongo.errors.DuplicateKeyError:
            raise ValidationFailed(
                [cls.serialize(document) for document in documents],
                message="One document already exists.",
            )

    @classmethod
    def validate_and_deserialize_update(cls, documents: List[dict]) -> dict:
        errors = {}

        for index, document in enumerate(documents):
            document_errors = cls.validate_update(document)
            if document_errors:
                errors[index] = document_errors
                continue

            if (
                not errors
            ):  # Skip deserialization in case errors were found as it will stop
                cls.deserialize_update(document)

        return errors

    @classmethod
    def validate_update(cls, document: dict) -> dict:
        """
        Validate a document update request.

        :param document: Updated version (partial) of a Mongo document.
        Each entry if composed of a field name associated to a value.
        :return: Validation errors that might have occurred. Empty if no error occurred.
        Entry would be composed of a field name associated to a list of error messages.
        """
        if document is None:
            return {"": ["No data provided."]}

        if not isinstance(document, dict):
            return {"": ["Must be a dictionary."]}

        new_document = copy.deepcopy(document)

        errors = {}

        updated_field_names = [
            field.name for field in cls.__fields__ if field.name in new_document
        ]
        unknown_fields = [
            field_name
            for field_name in new_document
            if field_name not in updated_field_names
        ]
        for unknown_field in unknown_fields:
            known_field, field_value = cls._to_known_field(
                unknown_field, new_document[unknown_field]
            )
            if known_field:
                new_document.setdefault(known_field.name, {}).update(field_value)
            elif not cls._skip_unknown_fields:
                errors.update({unknown_field: ["Unknown field"]})

        # Also ensure that primary keys will contain a valid value
        updated_fields = [
            field
            for field in cls.__fields__
            if field.name in new_document or field.is_primary_key
        ]
        for field in updated_fields:
            errors.update(field.validate_update(new_document))

        return errors

    @classmethod
    def deserialize_update(cls, document: dict):
        """
        Update this document values to values that can be inserted (updated) in Mongo.
        Remove unknown fields.

        :param document: Updated version (partial) of a Mongo document.
        Each entry if composed of a field name associated to a value.
        """
        updated_field_names = [
            field.name for field in cls.__fields__ if field.name in document
        ]
        unknown_fields = [
            field_name
            for field_name in document
            if field_name not in updated_field_names
        ]
        known_fields = {}

        for unknown_field in unknown_fields:
            known_field, field_value = cls._to_known_field(
                unknown_field, document[unknown_field]
            )
            del document[unknown_field]
            if known_field:
                known_fields.setdefault(known_field.name, {}).update(field_value)
            elif unknown_field not in cls._skip_log_for_unknown_fields:
                cls.logger.warning(f"Skipping unknown field {unknown_field}.")

        document_without_dot_notation = {**document, **known_fields}
        # Deserialize dot notation values
        for field in [field for field in cls.__fields__ if field.name in known_fields]:
            # Ensure that every provided field will be provided as deserialization might rely on another field
            field.deserialize_update(document_without_dot_notation)
            # Put back deserialized values as dot notation fields
            for inner_field_name, value in document_without_dot_notation[
                field.name
            ].items():
                document[f"{field.name}.{inner_field_name}"] = value

        updated_fields = [
            field
            for field in cls.__fields__
            if field.name in document or field.is_primary_key
        ]
        for field in updated_fields:
            field.deserialize_update(document)

    @classmethod
    def remove(cls, **filters) -> int:
        """
        Remove the document(s) matching those criteria.

        :param filters: Provided filters.
        Each entry if composed of a field name associated to a value.
        :returns Number of removed documents.
        """
        errors = cls.validate_remove(filters)
        if errors:
            raise ValidationFailed(filters, errors)

        cls.deserialize_query(filters)

        if cls.logger.isEnabledFor(logging.DEBUG):
            if filters:
                cls.logger.debug(f"Removing documents corresponding to {filters}...")
            else:
                cls.logger.debug("Removing all documents...")
        nb_removed = cls._delete_many(filters)
        if cls.logger.isEnabledFor(logging.DEBUG):
            cls.logger.debug(f"{nb_removed} documents removed.")
        return nb_removed

    @classmethod
    def validate_remove(cls, filters: dict) -> dict:
        """
        Validate a document(s) removal request.

        :param filters: Provided filters.
        Each entry if composed of a field name associated to a value.
        :return: Validation errors that might have occurred. Empty if no error occurred.
        Entry would be composed of a field name associated to a list of error messages.
        """
        return cls.validate_query(filters)

    @classmethod
    def _insert_many(cls, documents: List[dict]):
        cls.__collection__.insert_many(documents)
        if cls.audit_model:
            for document in documents:
                cls.audit_model.audit_add(document)

    @classmethod
    def _insert_one(cls, document: dict) -> dict:
        cls.__collection__.insert_one(document)
        if cls.audit_model:
            cls.audit_model.audit_add(document)
        return document

    @classmethod
    def _update_one(cls, document: dict) -> (dict, dict):
        document_keys = cls._to_primary_keys_model(document)
        previous_document = cls.__collection__.find_one(document_keys)
        if not previous_document:
            raise ValidationFailed(document_keys, message="The document to update could not be found.")

        new_document = cls.__collection__.find_one_and_update(
            document_keys,
            {"$set": document},
            return_document=pymongo.ReturnDocument.AFTER,
        )
        if cls.audit_model:
            cls.audit_model.audit_update(new_document)
        return previous_document, new_document

    @classmethod
    def _update_many(cls, documents: List[dict]) -> (List[dict], List[dict]):
        previous_documents = []
        new_documents = []
        for document in documents:
            document_keys = cls._to_primary_keys_model(document)
            previous_document = cls.__collection__.find_one(document_keys)
            if not previous_document:
                raise ValidationFailed(document_keys, message="The document to update could not be found.")

            new_document = cls.__collection__.find_one_and_update(
                document_keys,
                {"$set": document},
                return_document=pymongo.ReturnDocument.AFTER,
            )
            previous_documents.append(previous_document)
            new_documents.append(new_document)
            if cls.audit_model:
                cls.audit_model.audit_update(new_document)
        return previous_documents, new_documents

    @classmethod
    def _delete_many(cls, filters: dict) -> int:
        if cls.audit_model:
            cls.audit_model.audit_remove(**filters)
        if filters == {}:
            cls.reset_counters()
        return cls.__collection__.delete_many(filters).deleted_count

    @classmethod
    def _to_primary_keys_model(cls, document: dict) -> dict:
        # TODO Compute primary key field names only once
        primary_key_field_names = [
            field.name for field in cls.__fields__ if field.is_primary_key
        ]
        return {
            field_name: value
            for field_name, value in document.items()
            if field_name in primary_key_field_names
        }

    @classmethod
    def description_dictionary(cls) -> Dict[str, str]:
        description = {"collection": cls.__collection_name__}
        for field in cls.__fields__:
            description[field.name] = field.name
        return description


def _load(
    database_connection_url: str, controllers: Iterable[CRUDController], **kwargs
) -> pymongo.database.Database:
    """
    Create all necessary tables and perform the link between models and underlying database connection.

    :param database_connection_url: URL formatted as a standard database connection string (Mandatory).
    :param controllers: List of CRUDController-like instances (Mandatory).
    :param kwargs: MongoClient constructor parameters.
    :return Mongo Database instance.
    """
    logger.info(f'Connecting to "{database_connection_url}" ...')
    database_name = os.path.basename(database_connection_url)
    if database_connection_url.startswith("mongomock"):
        import mongomock  # This is a test dependency only

        client = mongomock.MongoClient(**kwargs)
    else:
        # Connect is false to avoid thread-race when connecting upon creation of MongoClient (No servers found yet)
        client = pymongo.MongoClient(
            database_connection_url, connect=kwargs.pop("connect", False), **kwargs
        )
    if "?" in database_name:  # Remove server options from the database name if any
        database_name = database_name[: database_name.index("?")]
    logger.info(f"Connecting to {database_name} database...")
    base = client[database_name]
    server_info = client.server_info()
    if server_info:
        logger.debug(f"Server information: {server_info}")
        _server_versions.setdefault(base.name, server_info.get("version", ""))
    logger.debug(f"Creating models...")
    for controller in controllers:
        link(controller, base)
    return base


def _reset(base: pymongo.database.Database) -> None:
    """
    If the database was already created, then drop all tables and recreate them all.

    :param base: database object as returned by the _load method (Mandatory).
    """
    if base:
        for collection in base.list_collection_names():
            _reset_collection(base, collection)


def _reset_collection(base: pymongo.database.Database, collection: str) -> None:
    """
    Reset collection and keep indexes.

    :param base: database object as returned by the _load method (Mandatory).
    :param collection: name of the collection (Mandatory).
    """
    logger.info(f'Resetting all data related to "{collection}" collection...')
    nb_removed = base[collection].delete_many({}).deleted_count
    logger.info(f"{nb_removed} records deleted.")

    logger.info(f'Resetting counters."{collection}".')
    nb_removed = base["counters"].delete_many({"_id": collection}).deleted_count
    logger.info(f"{nb_removed} counter records deleted")


def _check(base: pymongo.database.Database) -> (str, dict):
    """
    Return Health checks for this Mongo database connection.

    :param base: database object as returned by the _load method (Mandatory).
    :return: A tuple with a string providing the status (pass, warn, fail), and the checks.
    """
    try:
        response = base.command("ping")
        return (
            "pass",
            {
                f"{base.name}:ping": {
                    "componentType": "datastore",
                    "observedValue": response,
                    "status": "pass",
                    "time": datetime.datetime.utcnow().isoformat(),
                }
            },
        )
    except Exception as e:
        return (
            "fail",
            {
                f"{base.name}:ping": {
                    "componentType": "datastore",
                    "status": "fail",
                    "time": datetime.datetime.utcnow().isoformat(),
                    "output": str(e),
                }
            },
        )
