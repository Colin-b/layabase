import enum
import datetime
from typing import Dict, List, Union

import pymongo.database
import iso8601
from bson.objectid import ObjectId
from bson.errors import BSONError

from layabase import ComparisonSigns, CRUDController


@enum.unique
class IndexType(enum.IntEnum):
    Unique = 1
    Other = 2


_operators = {
    ComparisonSigns.Greater: "$gt",
    ComparisonSigns.GreaterOrEqual: "$gte",
    ComparisonSigns.Lower: "$lt",
    ComparisonSigns.LowerOrEqual: "$lte",
}


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

    def __set_name__(self, owner, name):
        self.name = name
        if "_id" == self.name:
            self.field_type = ObjectId
        self._validate_query = self._get_query_validation_function()
        self._validate_insert = self._get_insert_update_validation_function()
        self._validate_update = self._get_insert_update_validation_function()
        self._deserialize_value = self._get_value_deserialization_function()

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
        return self.name

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
                    self._deserialize_value(value_in_list)
                    if value_in_list is not None
                    else None
                    for value_in_list in value
                ]
                comparison_filters = {}
                equality_values = []
                for mongo_value in mongo_values:
                    if isinstance(mongo_value, tuple):
                        comparison_filters.update(
                            {_operators[mongo_value[0]]: mongo_value[1]}
                        )
                    else:
                        equality_values.append(mongo_value)

                filters_on_field = []

                if self.get_default_value(filters) in equality_values:
                    filters_on_field.append({self.name: {"$exists": False}})

                if equality_values:
                    filters_on_field.append({self.name: {"$in": equality_values}})

                if comparison_filters:
                    filters_on_field.append({self.name: comparison_filters})

                if len(filters_on_field) == 1:
                    filters[self.name] = filters_on_field[0][self.name]
                else:
                    filters.setdefault("$or", []).extend(filters_on_field)
        else:
            mongo_value = self._deserialize_value(value)
            if self.get_default_value(filters) == mongo_value:
                or_filter = filters.setdefault("$or", [])
                or_filter.append({self.name: {"$exists": False}})
                or_filter.append({self.name: mongo_value})
            elif isinstance(mongo_value, tuple):
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
        return iso8601.parse_date(value) if isinstance(value, str) else value

    def _deserialize_date(self, value):
        """
        Convert this field value to the proper value that can be inserted in Mongo.
        :param value: Received field value.
        :return Mongo valid value.
        """
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
        return value if isinstance(value, ObjectId) else ObjectId(value)

    def _deserialize_int(self, value):
        """
        Convert this field value to the proper value that can be inserted in Mongo.

        :param value: Received field value.
        :return Mongo valid value.
        """
        return int(value) if isinstance(value, str) else value

    def _deserialize_float(self, value):
        """
        Convert this field value to the proper value that can be inserted in Mongo.

        :param value: Received field value.
        :return Mongo valid value.
        """
        return float(value) if isinstance(value, str) else value

    def _deserialize_str(self, value):
        """
        Convert this field value to the proper value that can be inserted in Mongo.

        :param value: Received field value.
        :return Mongo valid value.
        """
        return value if isinstance(value, str) else str(value)

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
        self._get_fields = get_fields or (lambda model_as_dict: self._default_fields)

        self._get_all_index_fields = get_index_fields or (
            (lambda model_as_dict: index_fields) if index_fields else self._get_fields
        )

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
        :return: A class describing every dictionary fields.
        """
        # Create a class to ensure Column name is set
        return type(f"{self.name}_DefaultDescriptionModel", (), self._default_fields)

    def _description_model(self, model_as_dict: dict):
        """
        :param model_as_dict: Data provided by the user.
        :return: A CRUDModel describing every dictionary fields.
        """
        from layabase._database_mongo import _CRUDModel

        return type(
            f"{self.name}_DescriptionModel",
            (_CRUDModel,),
            self._get_fields(model_as_dict),
        )

    def _index_description_model(self, model_as_dict: dict):
        """
        :param model_as_dict: Data provided by the user.
        :return: A CRUDModel describing every index fields.
        """
        from layabase._database_mongo import _CRUDModel

        return type(
            f"{self.name}_IndexDescriptionModel",
            (_CRUDModel,),
            self._get_all_index_fields(model_as_dict),
        )

    def _get_index_fields(
        self, index_type: IndexType, model_as_dict: Union[dict, None], prefix: str
    ) -> List[str]:
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

    def __set_name__(self, owner, name):
        super().__set_name__(owner, name)
        self.list_item_column.__set_name__(owner, name)

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
            values = document[self.name] or []
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
            if not self._store_none:
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


def link(controller: CRUDController, base: pymongo.database.Database):
    """
    Link controller related collection to provided database.

    :param base: As returned by layabase.load function
    """
    if controller.history:
        import layabase._versioning_mongo

        crud_model = layabase._versioning_mongo.VersionedCRUDModel
    else:
        from layabase._database_mongo import _CRUDModel

        crud_model = _CRUDModel

    class ControllerModel(
        controller.table_or_collection,
        crud_model,
        base=base,
        skip_name_check=controller.skip_name_check,
        skip_unknown_fields=controller.skip_unknown_fields,
        skip_update_indexes=controller.skip_update_indexes,
        skip_log_for_unknown_fields=controller.skip_log_for_unknown_fields,
    ):
        pass

    controller._model = ControllerModel

    if controller.audit:
        from layabase._audit_mongo import _create_from

        ControllerModel.audit_model = _create_from(
            mixin=controller.table_or_collection,
            model=ControllerModel,
            base=base,
            retrieve_user=controller.retrieve_user,
        )

    controller._model_description_dictionary = ControllerModel.description_dictionary()
