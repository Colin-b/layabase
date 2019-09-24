import copy
import datetime
import enum
import inspect
import json
import logging
import os.path
import pathlib
from typing import List, Dict, Union, Type

import iso8601
import pymongo
import pymongo.errors
import pymongo.database
from bson.errors import BSONError
from bson.json_util import dumps, loads
from bson.objectid import ObjectId
from flask_restplus import fields as flask_restplus_fields, reqparse, inputs
from layaberr import ValidationFailed, ModelCouldNotBeFound
from layabase.database import ComparisonSigns

logger = logging.getLogger(__name__)


_operators = {
    ComparisonSigns.Greater: "$gt",
    ComparisonSigns.GreaterOrEqual: "$gte",
    ComparisonSigns.Lower: "$lt",
    ComparisonSigns.LowerOrEqual: "$lte",
}


@enum.unique
class IndexType(enum.IntEnum):
    Unique = 1
    Other = 2


class Column:
    """
    Definition of a Mongo document field.
    This field is used to:
    - Validate a value.
    - Deserialize a value to a valid Mongo (BSON) one.
    - Serialize a Mongo (BSON) value to a valid JSON one.
    """

    def __init__(self, field_type=None, **kwargs):
        """

        :param field_type: Python field type. Default to str.

        :param choices: Restrict valid values. Only for int, float, str and Enum fields.
        Should be a list or a function (without parameters) returning a list.
        Each list item should be of field type.
        None by default, or all enum values in case of an Enum field.
        :param counter: Custom counter definition. Only for auto incremented fields.
        Should be a tuple or a function (without parameters) returning a tuple. Content should be:
         - Counter name (field name by default), string value.
         - Counter category (table name by default), string value.
        :param default_value: Default field value returned to the client if field is not set.
        Should be of field type.
        None by default.
        :param get_default_value: Function returning default field value returned to the client if field is not set.
        Should be a function (with dictionary as parameter) returning a value of field type.
        Return default_value by default.
        :param description: Field description used in Swagger and in error messages.
        Should be a string value. Default to None.
        :param index_type: If and how this field should be indexed.
        Value should be one of IndexType enum. Default to None (not indexed).
        Parameter does not need to be provided if field is a primary key.
        :param allow_none_as_filter: If None value should be kept in queries (GET/DELETE).
        Should be a boolean value. Default to False.
        :param is_primary_key: If this field value is not allowed to be modified after insert.
        Should be a boolean value. Default to False (field value can always be modified).
        index_type will be IndexType.Unique if field is primary_key.
        :param is_nullable: If field value is optional.
        Should be a boolean value.
        Default to True if field is not a primary key.
        Default to True if field has a default value.
        Default to True (for insert only) if field value should auto increment.
        Otherwise default to False.
        Note that it is not allowed to force False if field has a default value or if value should auto increment.
        :param is_required: If field value must be specified in client requests. Use it to avoid heavy requests.
        Should be a boolean value. Default to False.
        :param should_auto_increment: If field should be auto incremented. Only for integer fields.
        Should be a boolean value. Default to False.
        :param min_value: Minimum value for a number field.
        :param max_value: Maximum value for a number field.
        :param min_length: Minimum value length. Only for integer, list or dict fields.
        Should be an integer value. Default to None (no minimum length).
        :param max_length: Maximum value length. Only for integer, list or dict fields.
        Should be an integer value. Default to None (no maximum length).
        :param example: Sample value. Should be of the field type.
        Default to None (default sample will be generated).
        :param store_none: If field value should be stored if None and None is a valid value. Should be a boolean.
        Default to False (None values will not be stored to save space).
        :param allow_comparison_signs: If field can be queries with ComparisonSign. Should be a boolean.
        Default to False (only equality can be queried).
        """
        self.field_type = field_type or str
        name = kwargs.pop("name", None)
        if name:
            self._update_name(name)
        self.get_choices = self._to_get_choices(kwargs.pop("choices", None))
        self.get_counter = self._to_get_counter(kwargs.pop("counter", None))
        self.default_value = kwargs.pop("default_value", None)
        self.get_default_value = self._to_get_default_value(
            kwargs.pop("get_default_value", None)
        )
        self.description = kwargs.pop("description", None)
        self.index_type = kwargs.pop("index_type", None)
        self.allow_none_as_filter: bool = bool(
            kwargs.pop("allow_none_as_filter", False)
        )
        self.should_auto_increment: bool = bool(
            kwargs.pop("should_auto_increment", False)
        )
        self.is_required: bool = bool(kwargs.pop("is_required", False))
        self.min_value = kwargs.pop("min_value", None)
        self.max_value = kwargs.pop("max_value", None)
        self.min_length: int = kwargs.pop("min_length", None)
        if self.min_length is not None:
            self.min_length = int(self.min_length)
        self.max_length: int = kwargs.pop("max_length", None)
        if self.max_length is not None:
            self.max_length = int(self.max_length)
        self._example = kwargs.pop("example", None)
        self._store_none: bool = bool(kwargs.pop("store_none", False))
        self.is_primary_key: bool = bool(kwargs.pop("is_primary_key", False))
        self.allow_comparison_signs = bool(kwargs.pop("allow_comparison_signs", False))
        if self.is_primary_key:
            if self.index_type:
                raise Exception(
                    "Primary key fields are supposed to be indexed as unique."
                )
            self.index_type = IndexType.Unique
        is_nullable = bool(kwargs.pop("is_nullable", True))
        if not is_nullable:
            if self.should_auto_increment:
                raise Exception(
                    "A field cannot be mandatory and auto incremented at the same time."
                )
            if self.default_value:
                raise Exception(
                    "A field cannot be mandatory and having a default value at the same time."
                )
            self._is_nullable_on_insert = False
            self._is_nullable_on_update = False
        else:
            # Field will be optional only if it is not a primary key without default value and not auto incremented
            self._is_nullable_on_insert = (
                not self.is_primary_key
                or self.default_value
                or self.should_auto_increment
            )
            # Field will be optional only if it is not a primary key without default value
            self._is_nullable_on_update = not self.is_primary_key or self.default_value
        self._check_parameters_validity()

    def _check_parameters_validity(self):
        if self.should_auto_increment and self.field_type is not int:
            raise Exception("Only int fields can be auto incremented.")
        if self.min_value is not None and not isinstance(
            self.min_value, self.field_type
        ):
            raise Exception(f"Minimum value should be of {self.field_type} type.")
        if self.max_value is not None:
            if not isinstance(self.max_value, self.field_type):
                raise Exception(f"Maximum value should be of {self.field_type} type.")
            if self.min_value is not None and self.max_value < self.min_value:
                raise Exception(
                    "Maximum value should be superior or equals to minimum value"
                )
        if self.min_length is not None and self.min_length < 0:
            raise Exception("Minimum length should be positive")
        if self.max_length is not None:
            if self.max_length < 0:
                raise Exception("Maximum length should be positive")
            if self.min_length is not None and self.max_length < self.min_length:
                raise Exception(
                    "Maximum length should be superior or equals to minimum length"
                )
        if self._example is not None and not isinstance(self._example, self.field_type):
            raise Exception("Example must be of field type.")

    def _update_name(self, name: str) -> "Column":
        if "." in name:
            raise Exception(
                f"{name} is not a valid name. Dots are not allowed in Mongo field names."
            )
        if name and (name[0] == " " or name[-1] == " "):
            raise Exception(
                f"{name} is not a valid name. Spaces are not allowed at start or end of field names."
            )
        self.name = name
        if "_id" == self.name:
            self.field_type = ObjectId
        self._validate_query = self._get_query_validation_function()
        self._validate_insert = self._get_insert_update_validation_function()
        self._validate_update = self._get_insert_update_validation_function()
        self._deserialize_value = self._get_value_deserialization_function()
        return self

    def _to_get_counter(self, counter):
        if counter:
            return counter if callable(counter) else lambda model_as_dict: counter
        return lambda model_as_dict: (self.name,)

    def _to_get_choices(self, choices):
        """
        Return a function without arguments returning the choices.
        :param choices: A list of choices or a function providing choices (or None).
        """
        if choices:
            return choices if callable(choices) else lambda: choices
        elif isinstance(self.field_type, enum.EnumMeta):
            return lambda: list(self.field_type.__members__.keys())
        return lambda: None

    def _to_get_default_value(self, get_default_value):
        return (
            get_default_value
            if get_default_value
            else lambda model_as_dict: self.default_value
        )

    def __str__(self):
        return f"{self.name}"

    def validate_query(self, filters: dict) -> dict:
        """
        Validate this field for a get or delete request.

        :param filters: Provided filters.
        Each entry if composed of a field name associated to a value.
        This field might not be in it.
        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        value = filters.get(self.name)
        if value is None:
            if self.is_required:
                return {self.name: ["Missing data for required field."]}
            return {}
        # Allow to specify a list of values when querying a field
        if isinstance(value, list) and self.field_type != list:
            errors = {}
            for value_in_list in value:
                errors.update(self._validate_query(value_in_list))
            return errors
        else:
            return self._validate_query(value)

    def validate_insert(self, document: dict) -> dict:
        """
        Validate this field for a document insertion request.

        :param document: Mongo to be document.
        Each entry if composed of a field name associated to a value.
        This field might not be in it.
        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        value = document.get(self.name)
        if value is None:
            if not self._is_nullable_on_insert:
                return {self.name: ["Missing data for required field."]}
            return {}
        return self._validate_insert(value)

    def validate_update(self, document: dict) -> dict:
        """
        Validate this field for a document update request.

        :param document: Updated version (partial) of a Mongo document.
        Each entry if composed of a field name associated to a value.
        This field might not be in it.
        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        value = document.get(self.name)
        if value is None:
            if not self._is_nullable_on_update:
                return {self.name: ["Missing data for required field."]}
            return {}
        return self._validate_update(value)

    def _get_query_validation_function(self) -> callable:
        """
        Return the function used to validate values on this field.
        """
        if self.field_type == datetime.datetime:
            return self._validate_query_date_time
        elif issubclass(datetime.date, self.field_type):
            return self._validate_query_date
        elif isinstance(self.field_type, enum.EnumMeta):
            return self._validate_enum
        elif self.field_type == ObjectId:
            return self._validate_object_id
        elif self.field_type == str:
            return self._validate_str
        elif self.field_type == list:
            return self._validate_list
        elif self.field_type == dict:
            return self._validate_dict
        elif self.field_type == int:
            return self._validate_query_int
        elif self.field_type == float:
            return self._validate_query_float
        else:
            return self._validate_type

    def _get_insert_update_validation_function(self) -> callable:
        """
        Return the function used to validate values on this field.
        """
        if self.field_type == datetime.datetime:
            return self._validate_date_time
        elif issubclass(datetime.date, self.field_type):
            return self._validate_date
        elif isinstance(self.field_type, enum.EnumMeta):
            return self._validate_enum
        elif self.field_type == ObjectId:
            return self._validate_object_id
        elif self.field_type == str:
            return self._validate_str
        elif self.field_type == list:
            return self._validate_list
        elif self.field_type == dict:
            return self._validate_dict
        elif self.field_type == int:
            return self._validate_int
        elif self.field_type == float:
            return self._validate_float
        else:
            return self._validate_type

    def _validate_date_time(self, value) -> dict:
        """
        Validate this value for this datetime field.
        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        if isinstance(value, str):
            try:
                value = iso8601.parse_date(value)
            except iso8601.ParseError:
                return {self.name: ["Not a valid datetime."]}

        return self._validate_type(value)

    def _validate_date(self, value) -> dict:
        """
        Validate this value for this date field.
        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        if isinstance(value, str):
            try:
                value = iso8601.parse_date(value).date()
            except iso8601.ParseError:
                return {self.name: ["Not a valid date."]}

        return self._validate_type(value)

    def _validate_query_date_time(self, value) -> dict:
        """
        Validate this value for this datetime field.

        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        # When using comparison signs, the value is a tuple containing the comparison sign and the value. ex: (ComparisonSigns.Lower, 124)
        if self.allow_comparison_signs and isinstance(value, tuple):
            value = value[1]

        return self._validate_date_time(value)

    def _validate_query_date(self, value) -> dict:
        """
        Validate this value for this date field.

        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        # When using comparison signs, the value is a tuple containing the comparison sign and the value. ex: (ComparisonSigns.Lower, 124)
        if self.allow_comparison_signs and isinstance(value, tuple):
            value = value[1]

        return self._validate_date(value)

    def _validate_enum(self, value) -> dict:
        """
        Validate this value for this Enum field.

        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        if isinstance(value, str):
            if value not in self.get_choices():
                return {
                    self.name: [f'Value "{value}" is not within {self.get_choices()}.']
                }
            return {}  # Consider string values valid for Enum type

        return self._validate_type(value)

    def _validate_object_id(self, value) -> dict:
        """
        Validate this value for this ObjectId field.

        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        if not isinstance(value, ObjectId):
            try:
                value = ObjectId(value)
            except BSONError as e:
                return {self.name: [str(e)]}

        return self._validate_type(value)

    def _validate_str(self, value) -> dict:
        """
        Validate this value for this str field.

        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        if (isinstance(value, int) and not isinstance(value, bool)) or isinstance(
            value, float
        ):
            value = str(value)
        if isinstance(value, str):
            if self.get_choices() and value not in self.get_choices():
                return {
                    self.name: [f'Value "{value}" is not within {self.get_choices()}.']
                }
            if self.min_length and len(value) < self.min_length:
                return {
                    self.name: [
                        f'Value "{value}" is too small. Minimum length is {self.min_length}.'
                    ]
                }
            if self.max_length and len(value) > self.max_length:
                return {
                    self.name: [
                        f'Value "{value}" is too big. Maximum length is {self.max_length}.'
                    ]
                }

        return self._validate_type(value)

    def _validate_list(self, value) -> dict:
        """
        Validate this value for this list field.

        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        if isinstance(value, list):
            if self.min_length and len(value) < self.min_length:
                return {
                    self.name: [
                        f"{value} does not contains enough values. Minimum length is {self.min_length}."
                    ]
                }
            if self.max_length and len(value) > self.max_length:
                return {
                    self.name: [
                        f"{value} contains too many values. Maximum length is {self.max_length}."
                    ]
                }

        return self._validate_type(value)

    def _validate_dict(self, value) -> dict:
        """
        Validate this value for this dict field.

        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        if isinstance(value, dict):
            if self.min_length and len(value) < self.min_length:
                return {
                    self.name: [
                        f"{value} does not contains enough values. Minimum length is {self.min_length}."
                    ]
                }
            if self.max_length and len(value) > self.max_length:
                return {
                    self.name: [
                        f"{value} contains too many values. Maximum length is {self.max_length}."
                    ]
                }

        return self._validate_type(value)

    def _validate_int(self, value) -> dict:
        """
        Validate this value for this int field.
        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        if isinstance(value, str):
            try:
                value = int(value)
            except ValueError:
                return {self.name: [f"Not a valid int."]}
        if isinstance(value, int):
            if self.get_choices() and value not in self.get_choices():
                return {
                    self.name: [f'Value "{value}" is not within {self.get_choices()}.']
                }
            if self.min_value is not None and value < self.min_value:
                return {
                    self.name: [
                        f'Value "{value}" is too small. Minimum value is {self.min_value}.'
                    ]
                }
            if self.max_value is not None and value > self.max_value:
                return {
                    self.name: [
                        f'Value "{value}" is too big. Maximum value is {self.max_value}.'
                    ]
                }

        return self._validate_type(value)

    def _validate_float(self, value) -> dict:
        """
        Validate this value for this float field.
        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        if isinstance(value, str):
            try:
                value = float(value)
            except ValueError:
                return {self.name: [f"Not a valid float."]}
        elif isinstance(value, int):
            value = float(value)
        if isinstance(value, float):
            if self.get_choices() and value not in self.get_choices():
                return {
                    self.name: [f'Value "{value}" is not within {self.get_choices()}.']
                }
            if self.min_value is not None and value < self.min_value:
                return {
                    self.name: [
                        f'Value "{value}" is too small. Minimum value is {self.min_value}.'
                    ]
                }
            if self.max_value is not None and value > self.max_value:
                return {
                    self.name: [
                        f'Value "{value}" is too big. Maximum value is {self.max_value}.'
                    ]
                }

        return self._validate_type(value)

    def _validate_query_int(self, value) -> dict:
        # When using comparison signs, the value is a tuple containing the comparison sign and the value. ex: (ComparisonSigns.Lower, 124)
        if self.allow_comparison_signs and isinstance(value, tuple):
            value = value[1]
        return self._validate_int(value)

    def _validate_query_float(self, value) -> dict:
        # When using comparison signs, the value is a tuple containing the comparison sign and the value. ex: (ComparisonSigns.Lower, 124)
        if self.allow_comparison_signs and isinstance(value, tuple):
            value = value[1]
        return self._validate_float(value)

    def _validate_type(self, value) -> dict:
        """
        Validate this value according to the expected field type.

        :return: Validation errors that might have occurred on this field. Empty if no error occurred.
        Entry would be composed of the field name associated to a list of error messages.
        """
        if not isinstance(value, self.field_type):
            return {self.name: [f"Not a valid {self.field_type.__name__}."]}

        return {}

    @staticmethod
    def _deserialize_comparison_signs_if_exists(comparison_sign, value):
        return {_operators[comparison_sign]: value}

    def deserialize_query(self, filters: dict):
        """
        Update this field value within provided filters to a value that can be queried in Mongo.

        :param filters: Provided filters.
        Each entry if composed of a field name associated to a value.
        This field might not be in it.
        """
        value = filters.pop(self.name, None)
        if value is None:
            if self.allow_none_as_filter:
                filters[self.name] = None
        # Allow to specify a list of values to query
        elif isinstance(value, list) and self.field_type != list:
            if value:  # Discard empty list as filter on non list field
                mongo_values = [
                    self._deserialize_value(value_in_list) for value_in_list in value
                ]
                if self.get_default_value(filters) in mongo_values:
                    or_filter = filters.setdefault("$or", [])
                    or_filter.append({self.name: {"$exists": False}})
                    or_filter.append({self.name: {"$in": mongo_values}})
                else:
                    mongo_filters = {}
                    other_values = []
                    for val in mongo_values:
                        if isinstance(val, tuple):
                            mongo_filters.update({_operators[val[0]]: val[1]})
                        else:
                            other_values.append(val)

                    if mongo_filters and other_values:
                        or_filter = filters.setdefault("$or", [])
                        or_filter.append({self.name: mongo_filters})
                        or_filter.append({self.name: {"$in": mongo_values}})
                    else:
                        filters[self.name] = (
                            {"$in": other_values} if other_values else mongo_filters
                        )
        else:
            mongo_value = self._deserialize_value(value)
            if self.get_default_value(filters) == mongo_value:
                or_filter = filters.setdefault("$or", [])
                or_filter.append({self.name: {"$exists": False}})
                or_filter.append({self.name: mongo_value})
            else:
                if isinstance(mongo_value, tuple):
                    filters[self.name] = {_operators[mongo_value[0]]: mongo_value[1]}
                else:
                    filters[self.name] = mongo_value

    def deserialize_insert(self, document: dict):
        """
        Update this field value within the document to a value that can be inserted in Mongo.

        :param document: Document that should be inserted.
        Each entry if composed of a field name associated to a value.
        This field might not be in it.
        """
        value = document.get(self.name)
        if value is None:
            if not self._store_none:
                # Ensure that None value are not stored to save space and allow to change default value.
                document.pop(self.name, None)
        else:
            document[self.name] = self._deserialize_value(value)

    def deserialize_update(self, document: dict):
        """
        Update this field value within the document to a value that can be inserted (updated) in Mongo.

        :param document: Updated version (partial) of a Mongo document.
        Each entry if composed of a field name associated to a value.
        This field might not be in it.
        """
        value = document.get(self.name)
        if value is None:
            if not self._store_none:
                # Ensure that None value are not stored to save space and allow to change default value.
                document.pop(self.name, None)
        else:
            document[self.name] = self._deserialize_value(value)

    def _get_value_deserialization_function(self) -> callable:
        """
        Return the function to convert values to the proper value that can be inserted in Mongo.
        """
        if self.field_type == datetime.datetime:
            return self._deserialize_date_time
        elif self.field_type == datetime.date:
            return self._deserialize_date
        elif isinstance(self.field_type, enum.EnumMeta):
            return self._deserialize_enum
        elif self.field_type == ObjectId:
            return self._deserialize_object_id
        elif self.field_type == int:
            return self._deserialize_int
        elif self.field_type == float:
            return self._deserialize_float
        elif self.field_type == str:
            return self._deserialize_str
        else:
            return lambda value: value

    def _deserialize_date_time(self, value):
        """
        Convert this field value to the proper value that can be inserted in Mongo.

        :param value: Received field value.
        :return Mongo valid value.
        """
        if value is None:
            return None

        if isinstance(value, str):
            value = iso8601.parse_date(value)

        return value

    def _deserialize_date(self, value):
        """
        Convert this field value to the proper value that can be inserted in Mongo.
        :param value: Received field value.
        :return Mongo valid value.
        """
        if value is None:
            return None

        if isinstance(value, str):
            value = iso8601.parse_date(value)
        elif isinstance(value, datetime.date):
            # dates cannot be stored in Mongo, use datetime instead
            if not isinstance(value, datetime.datetime):
                value = datetime.datetime.combine(value, datetime.datetime.min.time())
            # Ensure that time is not something else than midnight
            else:
                value = datetime.datetime.combine(
                    value.date(), datetime.datetime.min.time()
                )

        return value

    def _deserialize_enum(self, value):
        """
        Convert this field value to the proper value that can be inserted in Mongo.

        :param value: Received field value.
        :return Mongo valid value.
        """
        if value is None:
            return None

        # Enum cannot be stored in Mongo, use enum value instead
        if isinstance(value, enum.Enum):
            value = value.value
        elif isinstance(value, str):
            value = self.field_type[value].value

        return value

    def _deserialize_object_id(self, value):
        """
        Convert this field value to the proper value that can be inserted in Mongo.

        :param value: Received field value.
        :return Mongo valid value.
        """
        if value is None:
            return None

        if not isinstance(value, ObjectId):
            value = ObjectId(value)

        return value

    def _deserialize_int(self, value):
        """
        Convert this field value to the proper value that can be inserted in Mongo.

        :param value: Received field value.
        :return Mongo valid value.
        """
        if value is None:
            return None

        if isinstance(value, str):
            value = int(value)

        return value

    def _deserialize_float(self, value):
        """
        Convert this field value to the proper value that can be inserted in Mongo.

        :param value: Received field value.
        :return Mongo valid value.
        """
        if value is None:
            return None

        if isinstance(value, str):
            value = float(value)

        return value

    def _deserialize_str(self, value):
        """
        Convert this field value to the proper value that can be inserted in Mongo.

        :param value: Received field value.
        :return Mongo valid value.
        """
        if value is None:
            return None

        if not isinstance(value, str):
            value = str(value)

        return value

    def serialize(self, document: dict):
        """
        Update Mongo field value within this document to a valid JSON one.

        :param document: Document (as stored within database).
        """
        value = document.get(self.name)

        if value is None:
            document[self.name] = self.get_default_value(document)
        elif self.field_type == datetime.datetime:
            document[
                self.name
            ] = (
                value.isoformat()
            )  # TODO Time Offset is missing to be fully compliant with RFC
        elif self.field_type == datetime.date:
            document[self.name] = value.date().isoformat()
        elif isinstance(self.field_type, enum.EnumMeta):
            document[self.name] = self.field_type(value).name
        elif self.field_type == ObjectId:
            document[self.name] = str(value)

    def example(self):
        if self._example is not None:
            return self._example

        if self.default_value is not None:
            return self.default_value

        return self.get_choices()[0] if self.get_choices() else self._default_example()

    def _default_example(self):
        """
        Return an Example value corresponding to this Mongodb field.
        """
        if self.field_type == int:
            return self.min_value if self.min_value else 1
        if self.field_type == float:
            return 1.4
        if self.field_type == bool:
            return True
        if self.field_type == datetime.date:
            return "2017-09-24"
        if self.field_type == datetime.datetime:
            return "2017-09-24T15:36:09"
        if self.field_type == list:
            return (
                [f"Sample {i}" for i in range(self.min_length)]
                if self.min_length
                else [f"1st {self.name} sample", f"2nd {self.name} sample"][
                    : self.max_length or 2
                ]
            )
        if self.field_type == dict:
            return {
                f"1st {self.name} key": f"1st {self.name} sample",
                f"2nd {self.name} key": f"2nd {self.name} sample",
            }
        if self.field_type == ObjectId:
            return "1234567890QBCDEF01234567"
        return (
            "X" * self.min_length
            if self.min_length
            else f"sample {self.name}"[: self.max_length or 1000]
        )


class DictColumn(Column):
    """
    Definition of a Mongo document dictionary field.
    If you do not want to validate the content of this dictionary use a Column(dict) instead.
    """

    def __init__(
        self,
        fields: Dict[str, Column] = None,
        get_fields=None,
        index_fields: Dict[str, Column] = None,
        get_index_fields=None,
        **kwargs,
    ):
        """
        :param fields: Static definition of this dictionary.
        Keys are field names and associated values are Column.
        Default to an empty dictionary.
        :param get_fields: Function returning a definition of this dictionary.
        Should be a function (with dictionary as parameter) returning a dictionary.
        Keys are field names and associated values are Column.
        Default to returning fields.
        :param index_fields: Definition of all possible dictionary fields.
        This is used to identify every possible index fields.
        Keys are field names and associated values are Column.
        Default to fields.
        :param get_index_fields: Function returning a definition of all possible dictionary fields.
        This is used to identify every possible index fields.
        Should be a function (with dictionary as parameter) returning a dictionary.
        Keys are field names and associated values are Column.
        Default to returning index_fields.
        :param default_value: Default field value returned to the client if field is not set.
        Should be a dictionary.
        Dictionary based on fields if field is nullable.
        :param get_default_value: Function returning default field value returned to the client if field is not set.
        Should be a function (with dictionary as parameter) returning a dictionary.
        Function returning a dictionary based on get_fields if field is nullable.
        :param description: Field description used in Swagger and in error messages.
        Should be a string value. Default to None.
        :param index_type: If and how this field should be indexed.
        Value should be one of IndexType enum. Default to None (not indexed).
        :param allow_none_as_filter: If None value should be kept in queries (GET/DELETE).
        Should be a boolean value. Default to False.
        :param is_primary_key: If this field value is not allowed to be modified after insert.
        Should be a boolean value. Default to False (field value can always be modified).
        :param is_nullable: If field value is optional.
        Should be a boolean value.
        Default to True if field is not a primary key.
        Default to True if field has a default value.
        Otherwise default to False.
        Note that it is not allowed to force False if field has a default value.
        :param is_required: If field value must be specified in client requests. Use it to avoid heavy requests.
        Should be a boolean value. Default to False.
        :param min_length: Minimum number of items.
        Should be an integer value. Default to None (no minimum length).
        :param max_length: Maximum number of items.
        Should be an integer value. Default to None (no maximum length).
        """
        kwargs.pop("field_type", None)

        if not fields and not get_fields:
            raise Exception("fields or get_fields must be provided.")

        self._default_fields = fields or {}
        self._get_fields = (
            get_fields if get_fields else lambda model_as_dict: self._default_fields
        )

        if index_fields:
            self._get_all_index_fields = (
                get_index_fields
                if get_index_fields
                else lambda model_as_dict: index_fields
            )
            self._default_index_fields = index_fields
        else:
            self._get_all_index_fields = self._get_fields
            self._default_index_fields = self._default_fields

        if bool(
            kwargs.get("is_nullable", True)
        ):  # Ensure that there will be a default value if field is nullable
            if "default_value" not in kwargs:
                kwargs["default_value"] = {
                    field_name: field.default_value
                    for field_name, field in self._default_fields.items()
                }
            if "get_default_value" not in kwargs:
                kwargs["get_default_value"] = lambda model_as_dict: {
                    field_name: field.get_default_value(model_as_dict)
                    for field_name, field in self._get_fields(model_as_dict).items()
                }

        Column.__init__(self, dict, **kwargs)

    def _default_description_model(self):
        """
        :return: A CRUDModel describing every dictionary fields.
        """

        class FakeModel(CRUDModel):
            pass

        for name, column in self._default_fields.items():
            column._update_name(name)
            FakeModel.__fields__.append(column)

        return FakeModel

    def _description_model(self, model_as_dict: dict):
        """
        :param model_as_dict: Data provided by the user.
        :return: A CRUDModel describing every dictionary fields.
        """

        class FakeModel(CRUDModel):
            pass

        for name, column in self._get_fields(model_as_dict).items():
            column._update_name(name)
            FakeModel.__fields__.append(column)

        return FakeModel

    def _default_index_description_model(self):
        """
        :return: A CRUDModel describing every index fields.
        """

        class FakeModel(CRUDModel):
            pass

        for name, column in self._default_index_fields.items():
            column._update_name(name)
            FakeModel.__fields__.append(column)

        return FakeModel

    def _index_description_model(self, model_as_dict: dict):
        """
        :param model_as_dict: Data provided by the user.
        :return: A CRUDModel describing every index fields.
        """

        class FakeModel(CRUDModel):
            pass

        for name, column in self._get_all_index_fields(model_as_dict).items():
            column._update_name(name)
            FakeModel.__fields__.append(column)

        return FakeModel

    def _get_index_fields(
        self, index_type: IndexType, model_as_dict: Union[dict, None], prefix: str
    ) -> List[str]:
        if model_as_dict is None:
            return self._default_index_description_model()._get_index_fields(
                index_type, None, f"{prefix}{self.name}."
            )
        return self._index_description_model(model_as_dict)._get_index_fields(
            index_type, model_as_dict, f"{prefix}{self.name}."
        )

    def validate_insert(self, model_as_dict: dict) -> dict:
        errors = Column.validate_insert(self, model_as_dict)
        if not errors:
            value = model_as_dict.get(self.name)
            if value is not None:
                try:
                    description_model = self._description_model(model_as_dict)
                    errors.update(
                        {
                            f"{self.name}.{field_name}": field_errors
                            for field_name, field_errors in description_model.validate_insert(
                                value
                            ).items()
                        }
                    )
                except Exception as e:
                    errors[self.name] = [str(e)]
        return errors

    def deserialize_insert(self, document: dict):
        value = document.get(self.name)
        if value is None:
            # Ensure that None value are not stored to save space and allow to change default value.
            document.pop(self.name, None)
        else:
            self._description_model(document).deserialize_insert(value)

    def validate_update(self, document: dict) -> dict:
        errors = Column.validate_update(self, document)
        if not errors:
            value = document.get(self.name)
            if value is not None:
                try:
                    description_model = self._description_model(document)
                    errors.update(
                        {
                            f"{self.name}.{field_name}": field_errors
                            for field_name, field_errors in description_model.validate_update(
                                value
                            ).items()
                        }
                    )
                except Exception as e:
                    errors[self.name] = [str(e)]
        return errors

    def deserialize_update(self, document: dict):
        value = document.get(self.name)
        if value is None:
            # Ensure that None value are not stored to save space and allow to change default value.
            document.pop(self.name, None)
        else:
            self._description_model(document).deserialize_update(value)

    def validate_query(self, filters: dict) -> dict:
        errors = Column.validate_query(self, filters)
        if not errors:
            value = filters.get(self.name)
            if value is not None:
                try:
                    description_model = self._description_model(filters)
                    errors.update(
                        {
                            f"{self.name}.{field_name}": field_errors
                            for field_name, field_errors in description_model.validate_query(
                                value
                            ).items()
                        }
                    )
                except Exception as e:
                    errors[self.name] = [str(e)]
        return errors

    def deserialize_query(self, filters: dict):
        value = filters.get(self.name)
        if value is None:
            if not self.allow_none_as_filter:
                filters.pop(self.name, None)
        else:
            self._description_model(filters).deserialize_query(value)

    def serialize(self, document: dict):
        value = document.get(self.name)
        if value is None:
            document[self.name] = self.get_default_value(document)
        else:
            self._description_model(document).serialize(value)

    def example(self):
        return {
            field_name: dict_field.example()
            for field_name, dict_field in self._default_fields.items()
        }


class ListColumn(Column):
    """
    Definition of a Mongo document list field.
    This list should only contains items of a specified type.
    If you do not want to validate the content of this list use a Column(list) instead.
    """

    def __init__(self, list_item_type: Column, **kwargs):
        """
        :param list_item_type: Column describing an element of this list.
        :param sorted: If content should be sorted. Insertion order is kept by default.

        :param default_value: Default field value returned to the client if field is not set.
        Should be a dictionary or a function (with dictionary as parameter) returning a dictionary.
        None by default.
        :param description: Field description used in Swagger and in error messages.
        Should be a string value. Default to None.
        :param index_type: If and how this field should be indexed.
        Value should be one of IndexType enum. Default to None (not indexed).
        :param allow_none_as_filter: If None value should be kept in queries (GET/DELETE).
        Should be a boolean value. Default to False.
        :param is_primary_key: If this field value is not allowed to be modified after insert.
        Should be a boolean value. Default to False (field value can always be modified).
        :param is_nullable: If field value is optional.
        Should be a boolean value.
        Default to True if field is not a primary key.
        Default to True if field has a default value.
        Otherwise default to False.
        Note that it is not allowed to force False if field has a default value.
        :param is_required: If field value must be specified in client requests. Use it to avoid heavy requests.
        Should be a boolean value. Default to False.
        :param min_length: Minimum number of items.
        :param max_length: Maximum number of items.
        """
        kwargs.pop("field_type", None)
        self.list_item_column = list_item_type
        self.sorted = bool(kwargs.pop("sorted", False))
        Column.__init__(self, list, **kwargs)

    def _update_name(self, name: str) -> "Column":
        Column._update_name(self, name)
        self.list_item_column._update_name(name)
        return self

    def validate_insert(self, document: dict) -> dict:
        errors = Column.validate_insert(self, document)
        if not errors:
            values = document.get(self.name) or []
            for index, value in enumerate(values):
                document_with_list_item = {**document, self.name: value}
                list_item_errors = self.list_item_column.validate_insert(
                    document_with_list_item
                )
                errors.update(
                    {
                        f"{field_name}[{index}]": field_errors
                        for field_name, field_errors in list_item_errors.items()
                    }
                )
        return errors

    def deserialize_insert(self, document: dict):
        values = document.get(self.name)
        if values is None:
            # Ensure that None value are not stored to save space and allow to change default value.
            document.pop(self.name, None)
        else:
            new_values = []
            for value in values:
                document_with_list_item = {**document, self.name: value}
                self.list_item_column.deserialize_insert(document_with_list_item)
                if self.name in document_with_list_item:
                    new_values.append(document_with_list_item[self.name])

            document[self.name] = sorted(new_values) if self.sorted else new_values

    def validate_update(self, document: dict) -> dict:
        errors = Column.validate_update(self, document)
        if not errors:
            values = document[self.name]
            for index, value in enumerate(values):
                document_with_list_item = {**document, self.name: value}
                list_item_errors = self.list_item_column.validate_update(
                    document_with_list_item
                )
                errors.update(
                    {
                        f"{field_name}[{index}]": field_errors
                        for field_name, field_errors in list_item_errors.items()
                    }
                )
        return errors

    def deserialize_update(self, document: dict):
        values = document.get(self.name)
        if values is None:
            # Ensure that None value are not stored to save space and allow to change default value.
            document.pop(self.name, None)
        else:
            new_values = []
            for value in values:
                document_with_list_item = {**document, self.name: value}
                self.list_item_column.deserialize_update(document_with_list_item)
                if self.name in document_with_list_item:
                    new_values.append(document_with_list_item[self.name])

            document[self.name] = sorted(new_values) if self.sorted else new_values

    def validate_query(self, filters: dict) -> dict:
        errors = Column.validate_query(self, filters)
        if not errors:
            values = filters.get(self.name) or []
            for index, value in enumerate(values):
                filters_with_list_item = {**filters, self.name: value}
                list_item_errors = self.list_item_column.validate_query(
                    filters_with_list_item
                )
                errors.update(
                    {
                        f"{field_name}[{index}]": field_errors
                        for field_name, field_errors in list_item_errors.items()
                    }
                )
        return errors

    def deserialize_query(self, filters: dict):
        values = filters.get(self.name)
        if values is None:
            if not self.allow_none_as_filter:
                filters.pop(self.name, None)
        else:
            new_values = []
            for value in values:
                filters_with_list_item = {**filters, self.name: value}
                self.list_item_column.deserialize_query(filters_with_list_item)
                if self.name in filters_with_list_item:
                    new_values.append(filters_with_list_item[self.name])

            filters[self.name] = new_values

    def serialize(self, document: dict):
        values = document.get(self.name)
        if values is None:
            document[self.name] = self.get_default_value(document)
        else:
            # TODO use list comprehension
            new_values = []
            for value in values:
                document_with_list_item = {**document, self.name: value}
                self.list_item_column.serialize(document_with_list_item)
                new_values.append(document_with_list_item[self.name])

            document[self.name] = new_values

    def example(self):
        return [self.list_item_column.example()]


_server_versions: Dict[str, str] = {}


class CRUDModel:
    """
    Class providing CRUD helper methods for a Mongo model.
    __collection__ class property must be specified in Model.
    __counters__ class property must be specified in Model.
    Calling load_from(...) will provide you those properties.
    """

    __tablename__: str = None  # Name of the collection described by this model
    __collection__: pymongo.collection.Collection = None  # Mongo collection
    __counters__: pymongo.collection.Collection = None  # Mongo counters collection (to increment fields)
    __fields__: List[Column] = []  # All Mongo fields within this model
    audit_model: Type["CRUDModel"] = None
    _skip_unknown_fields: bool = True
    _skip_log_for_unknown_fields: List[str] = []
    logger = None
    _server_version: str = ""

    def __init_subclass__(
        cls,
        base: pymongo.database.Database = None,
        table_name: str = None,
        audit: bool = False,
        **kwargs,
    ):
        cls._skip_unknown_fields = kwargs.pop("skip_unknown_fields", True)
        cls._skip_log_for_unknown_fields = kwargs.pop("skip_log_for_unknown_fields", [])
        skip_name_check = kwargs.pop("skip_name_check", False)
        skip_update_indexes = kwargs.pop("skip_update_indexes", False)
        super().__init_subclass__(**kwargs)
        cls.__tablename__ = table_name
        cls.logger = logging.getLogger(f"{__name__}.{table_name}")
        cls.__fields__ = [
            field._update_name(field_name)
            for field_name, field in inspect.getmembers(cls)
            if isinstance(field, Column)
        ]
        if base is not None:  # Allow to not provide base to create fake models
            if not skip_name_check and cls._is_forbidden():
                raise Exception(f"{cls.__tablename__} is a reserved collection name.")
            cls.__collection__ = base[cls.__tablename__]
            cls.__counters__ = base["counters"]
            cls._server_version = _server_versions.get(base.name, "")
            if not skip_update_indexes:
                cls.update_indexes()
        if audit:
            from layabase.audit_mongo import _create_from

            cls.audit_model = _create_from(cls, base)
        else:
            # Ensure no circular reference when creating the audit
            cls.audit_model = None

    @classmethod
    def get_primary_keys(cls) -> List[str]:
        return [field.name for field in cls.__fields__ if field.is_primary_key]

    @classmethod
    def _is_forbidden(cls):
        # Counters collection is managed by pycommon_database
        # Audit collections are managed by pycommon_database
        return (
            not cls.__tablename__
            or "counters" == cls.__tablename__
            or cls.__tablename__.startswith("audit")
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
        index_name = f"idx{cls.__tablename__}"
        unique_index_name = f"uidx{cls.__tablename__}"
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
                    f"uidx{cls.__tablename__}"
                    if index_type == IndexType.Unique
                    else f"idx{cls.__tablename__}"
                )
                cls.logger.info(
                    f"Create {index_name} {index_type.name} index on {cls.__tablename__} using {criteria} criteria."
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
            cls.logger.debug(
                f'{len(list(documents)) if documents else "No corresponding"} documents retrieved.'
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
            raise ValidationFailed(documents, message="Must be a list.")

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
            raise ValidationFailed(document, message="Must be a dictionary.")

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
        except pymongo.errors.BulkWriteError as e:
            raise ValidationFailed(documents, message=str(e.details))
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
            raise ValidationFailed(document, message="Must be a dictionary.")

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
                cls.logger.debug(f"Removing all documents...")
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
            raise ModelCouldNotBeFound(document_keys)

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
                raise ModelCouldNotBeFound(document_keys)

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
    def query_get_parser(cls):
        query_get_parser = cls._query_parser()
        query_get_parser.add_argument("limit", type=inputs.positive)
        query_get_parser.add_argument("offset", type=inputs.natural)
        return query_get_parser

    @classmethod
    def query_get_history_parser(cls):
        query_get_hist_parser = cls._query_parser()
        query_get_hist_parser.add_argument("limit", type=inputs.positive)
        query_get_hist_parser.add_argument("offset", type=inputs.natural)
        return query_get_hist_parser

    @classmethod
    def query_delete_parser(cls):
        return cls._query_parser()

    @classmethod
    def query_rollback_parser(cls):
        pass  # Only VersionedCRUDModel allows rollback

    @classmethod
    def _query_parser(cls):
        query_parser = reqparse.RequestParser()
        for field in cls.__fields__:
            cls._add_field_to_query_parser(query_parser, field)
        return query_parser

    @classmethod
    def _add_field_to_query_parser(cls, query_parser, field: Column, prefix=""):
        if isinstance(field, DictColumn):
            # Describe every dict column field as dot notation
            for inner_field in field._default_description_model().__fields__:
                cls._add_field_to_query_parser(
                    query_parser, inner_field, f"{field.name}."
                )
        elif isinstance(field, ListColumn):
            # Note that List of dict or list of list might be wrongly parsed
            query_parser.add_argument(
                f"{prefix}{field.name}",
                required=field.is_required,
                type=_get_python_type(field.list_item_column),
                action="append",
                store_missing=not field.allow_none_as_filter,
            )
        elif field.field_type == list:
            query_parser.add_argument(
                f"{prefix}{field.name}",
                required=field.is_required,
                type=str,  # Consider anything as valid, thus consider as str in query
                action="append",
                store_missing=not field.allow_none_as_filter,
            )
        else:
            query_parser.add_argument(
                f"{prefix}{field.name}",
                required=field.is_required,
                type=_get_python_type(field),
                action="append",  # Allow to provide multiple values in queries
                store_missing=not field.allow_none_as_filter,
            )

    @classmethod
    def description_dictionary(cls) -> dict:
        description = {"collection": cls.__tablename__}
        for field in cls.__fields__:
            description[field.name] = field.name
        return description

    @classmethod
    def json_post_model(cls, namespace):
        return cls._model_with_all_fields(namespace)

    @classmethod
    def json_put_model(cls, namespace):
        return cls._model_with_all_fields(namespace)

    @classmethod
    def get_response_model(cls, namespace):
        return cls._model_with_all_fields(namespace)

    @classmethod
    def get_history_response_model(cls, namespace):
        return cls._model_with_all_fields(namespace)

    @classmethod
    def _model_with_all_fields(cls, namespace):
        return namespace.model(cls.__name__, cls._flask_restplus_fields(namespace))

    @classmethod
    def _flask_restplus_fields(cls, namespace) -> dict:
        return {
            field.name: cls._to_flask_restplus_field(namespace, field)
            for field in cls.__fields__
        }

    @classmethod
    def _to_flask_restplus_field(cls, namespace, field: Column):
        if isinstance(field, DictColumn):
            dict_fields = field._default_description_model()._flask_restplus_fields(
                namespace
            )
            if dict_fields:
                dict_model = namespace.model("_".join(dict_fields), dict_fields)
                # Nested field cannot contains nothing
                return flask_restplus_fields.Nested(
                    dict_model,
                    required=field.is_required,
                    example=field.example(),
                    description=field.description,
                    enum=field.get_choices(),
                    default=field.default_value,
                    readonly=field.should_auto_increment,
                    skip_none=True,
                )
            else:
                return flask_restplus_fields.Raw(
                    required=field.is_required,
                    example=field.example(),
                    description=field.description,
                    enum=field.get_choices(),
                    default=field.default_value,
                    readonly=field.should_auto_increment,
                )
        elif isinstance(field, ListColumn):
            return flask_restplus_fields.List(
                cls._to_flask_restplus_field(namespace, field.list_item_column),
                required=field.is_required,
                example=field.example(),
                description=field.description,
                enum=field.get_choices(),
                default=field.default_value,
                readonly=field.should_auto_increment,
                min_items=field.min_length,
                max_items=field.max_length,
            )
        elif field.field_type == list:
            return flask_restplus_fields.List(
                flask_restplus_fields.String,
                required=field.is_required,
                example=field.example(),
                description=field.description,
                enum=field.get_choices(),
                default=field.default_value,
                readonly=field.should_auto_increment,
                min_items=field.min_length,
                max_items=field.max_length,
            )
        elif field.field_type == int:
            return flask_restplus_fields.Integer(
                required=field.is_required,
                example=field.example(),
                description=field.description,
                enum=field.get_choices(),
                default=field.default_value,
                readonly=field.should_auto_increment,
                min=field.min_value,
                max=field.max_value,
            )
        elif field.field_type == float:
            return flask_restplus_fields.Float(
                required=field.is_required,
                example=field.example(),
                description=field.description,
                enum=field.get_choices(),
                default=field.default_value,
                readonly=field.should_auto_increment,
                min=field.min_value,
                max=field.max_value,
            )
        elif field.field_type == bool:
            return flask_restplus_fields.Boolean(
                required=field.is_required,
                example=field.example(),
                description=field.description,
                enum=field.get_choices(),
                default=field.default_value,
                readonly=field.should_auto_increment,
            )
        elif field.field_type == datetime.date:
            return flask_restplus_fields.Date(
                required=field.is_required,
                example=field.example(),
                description=field.description,
                enum=field.get_choices(),
                default=field.default_value,
                readonly=field.should_auto_increment,
            )
        elif field.field_type == datetime.datetime:
            return flask_restplus_fields.DateTime(
                required=field.is_required,
                example=field.example(),
                description=field.description,
                enum=field.get_choices(),
                default=field.default_value,
                readonly=field.should_auto_increment,
            )
        elif field.field_type == dict:
            return flask_restplus_fields.Raw(
                required=field.is_required,
                example=field.example(),
                description=field.description,
                enum=field.get_choices(),
                default=field.default_value,
                readonly=field.should_auto_increment,
            )
        else:
            return flask_restplus_fields.String(
                required=field.is_required,
                example=field.example(),
                description=field.description,
                enum=field.get_choices(),
                default=field.default_value,
                readonly=field.should_auto_increment,
                min_length=field.min_length,
                max_length=field.max_length,
            )

    @classmethod
    def flask_restplus_description_fields(cls) -> dict:
        exported_fields = {
            "collection": flask_restplus_fields.String(
                required=True, example="collection", description="Collection name"
            )
        }

        exported_fields.update(
            {
                field.name: flask_restplus_fields.String(
                    required=field.is_required,
                    example="column",
                    description=field.description,
                )
                for field in cls.__fields__
            }
        )
        return exported_fields


def _load(
    database_connection_url: str, create_models_func: callable, **kwargs
) -> pymongo.database.Database:
    """
    Create all necessary tables and perform the link between models and underlying database connection.

    :param database_connection_url: URL formatted as a standard database connection string (Mandatory).
    :param create_models_func: Function that will be called to create models and return them (instances of CRUDModel)
    (Mandatory).
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
    create_models_func(base)
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


def _dump(base: pymongo.database.Database, dump_path: str) -> None:
    """
    Dump the content of all the collections of the provided database as bson files in the provided directory

    :param base: database object as returned by the _load method (Mandatory).
    :param dump_path: directory name of where to store all the collections dumps.
    If the directory doesn't exist, it will be created (Mandatory).
    """
    logger.debug(f"dumping collections as bson...")
    pathlib.Path(dump_path).mkdir(parents=True, exist_ok=True)
    for collection in base.list_collection_names():
        dump_file = os.path.join(dump_path, f"{collection}.bson")
        logger.debug(f"dumping collection {collection} in {dump_file}")
        documents = list(base[collection].find({}))
        if documents:
            with open(dump_file, "w") as output:
                output.write(dumps(documents))


def _restore(base: pymongo.database.Database, restore_path: str) -> None:
    """
    Restore in the provided database the content of all the collections dumped in the provided path as bson.

    :param base: database object as returned by the _load method (Mandatory).
    :param restore_path: directory name of where all the collections dumps are stored (Mandatory).
    """
    logger.debug(f"restoring collections dumped as bson...")
    collections = [
        os.path.splitext(collection)[0]
        for collection in os.listdir(restore_path)
        if os.path.isfile(os.path.join(restore_path, collection))
        and os.path.splitext(collection)[1] == ".bson"
    ]
    for collection in collections:
        restore_file = os.path.join(restore_path, f"{collection}.bson")
        with open(restore_file, "r") as input:
            documents = loads(input.read())
            if len(documents) > 0:
                logger.debug(f"drop all records from collection {collection} if any")
                base[collection].delete_many({})
                logger.debug(f"import {restore_file} into collection {collection}")
                base[collection].insert_many(documents)


def _health_checks(base: pymongo.database.Database) -> (str, dict):
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


def _get_python_type(field: Column) -> callable:
    """
    Return a function taking a single parameter (the value) and converting to the required field type.
    """
    if field.field_type == bool:
        return inputs.boolean
    if field.field_type == datetime.date:
        return (
            _validate_date if field.allow_comparison_signs else inputs.date_from_iso8601
        )
    if field.field_type == datetime.datetime:
        return (
            _validate_date_time
            if field.allow_comparison_signs
            else inputs.datetime_from_iso8601
        )
    if isinstance(field.field_type, enum.EnumMeta):
        return str
    if field.field_type == dict:
        return json.loads
    if field.field_type == list:
        return json.loads
    if field.field_type == ObjectId:
        return str
    if field.field_type == float:
        return _validate_float if field.allow_comparison_signs else float
    if field.field_type == int:
        return _validate_int if field.allow_comparison_signs else int

    return field.field_type


def _validate_float(value):
    if isinstance(value, str):
        value = ComparisonSigns.deserialize(value)

        # When using comparison signs, the value is a tuple containing the comparison sign and the value. ex: (ComparisonSigns.Lower, 124)
        if isinstance(value, tuple):
            return value[0], float(value[1])

    return float(value)


_validate_float.__schema__ = {"type": "string"}


def _validate_int(value):
    if isinstance(value, str):
        value = ComparisonSigns.deserialize(value)

        # When using comparison signs, the value is a tuple containing the comparison sign and the value. ex: (ComparisonSigns.Lower, 124)
        if isinstance(value, tuple):
            return value[0], int(value[1])

    return int(value)


_validate_int.__schema__ = {"type": "string"}


def _validate_date_time(value):
    if isinstance(value, str):
        value = ComparisonSigns.deserialize(value)

        # When using comparison signs, the value is a tuple containing the comparison sign and the value. ex: (ComparisonSigns.Lower, 124)
        if isinstance(value, tuple):
            return value[0], iso8601.parse_date(value[1])

    return iso8601.parse_date(value)


_validate_date_time.__schema__ = {"type": "string"}


def _validate_date(value):
    if isinstance(value, str):
        value = ComparisonSigns.deserialize(value)

        # When using comparison signs, the value is a tuple containing the comparison sign and the value. ex: (ComparisonSigns.Lower, 124)
        if isinstance(value, tuple):
            return value[0], iso8601.parse_date(value[1])

    return iso8601.parse_date(value).date()


_validate_date.__schema__ = {"type": "string"}
