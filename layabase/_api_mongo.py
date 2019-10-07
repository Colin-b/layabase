import json
import enum
import datetime
from typing import Dict

import flask_restplus
import iso8601
from bson.objectid import ObjectId

from layabase import ComparisonSigns
from layabase.mongo import Column, DictColumn, ListColumn


def add_all_fields(collection, parser):
    for column in collection.__dict__.values():
        if isinstance(column, Column):
            _add_query_field(parser, column)


def _add_query_field(
    parser: flask_restplus.reqparse.RequestParser, field: Column, prefix=""
):
    if isinstance(field, DictColumn):
        # Describe every dict column field as dot notation
        for inner_field in field._default_description_model().__fields__:
            _add_query_field(parser, inner_field, f"{field.name}.")
    elif isinstance(field, ListColumn):
        # Note that List of dict or list of list might be wrongly parsed
        parser.add_argument(
            f"{prefix}{field.name}",
            required=field.is_required,
            type=_get_parser_type(field.list_item_column),
            action="append",
            store_missing=not field.allow_none_as_filter,
            location="args",
        )
    elif field.field_type == list:
        parser.add_argument(
            f"{prefix}{field.name}",
            required=field.is_required,
            type=str,  # Consider anything as valid, thus consider as str in query
            action="append",
            store_missing=not field.allow_none_as_filter,
            location="args",
        )
    else:
        parser.add_argument(
            f"{prefix}{field.name}",
            required=field.is_required,
            type=_get_parser_type(field),
            action="append",  # Allow to provide multiple values in queries
            store_missing=not field.allow_none_as_filter,
            location="args",
        )


def _get_parser_type(field: Column) -> callable:
    """
    Return a function taking a single parameter (the value) and converting to the required field type.
    """
    if field.field_type == bool:
        return flask_restplus.inputs.boolean
    if field.field_type == datetime.date:
        return (
            _validate_date
            if field.allow_comparison_signs
            else flask_restplus.inputs.date_from_iso8601
        )
    if field.field_type == datetime.datetime:
        return (
            _validate_date_time
            if field.allow_comparison_signs
            else flask_restplus.inputs.datetime_from_iso8601
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

        # When using comparison signs, the value is a tuple containing the comparison sign and the value.
        # ex: (ComparisonSigns.Lower, 124)
        if isinstance(value, tuple):
            return value[0], float(value[1])

    return float(value)


_validate_float.__schema__ = {"type": "string"}


def _validate_int(value):
    if isinstance(value, str):
        value = ComparisonSigns.deserialize(value)

        # When using comparison signs, the value is a tuple containing the comparison sign and the value.
        # ex: (ComparisonSigns.Lower, 124)
        if isinstance(value, tuple):
            return value[0], int(value[1])

    return int(value)


_validate_int.__schema__ = {"type": "string"}


def _validate_date_time(value):
    if isinstance(value, str):
        value = ComparisonSigns.deserialize(value)

        # When using comparison signs, the value is a tuple containing the comparison sign and the value.
        # ex: (ComparisonSigns.Lower, 124)
        if isinstance(value, tuple):
            return value[0], iso8601.parse_date(value[1])

    return iso8601.parse_date(value)


_validate_date_time.__schema__ = {"type": "string"}


def _validate_date(value):
    if isinstance(value, str):
        value = ComparisonSigns.deserialize(value)

        # When using comparison signs, the value is a tuple containing the comparison sign and the value.
        # ex: (ComparisonSigns.Lower, 124)
        if isinstance(value, tuple):
            return value[0], iso8601.parse_date(value[1])

    return iso8601.parse_date(value).date()


_validate_date.__schema__ = {"type": "string"}


def all_request_fields(
    collection, namespace: flask_restplus.Namespace
) -> Dict[str, flask_restplus.fields.Raw]:
    return {
        column.name: request_field(column, namespace)
        for column in collection.__dict__.values()
        if isinstance(column, Column)
    }


def request_field(
    field: Column, namespace: flask_restplus.Namespace
) -> flask_restplus.fields.Raw:
    if isinstance(field, DictColumn):
        dict_fields = all_request_fields(field._default_description_model(), namespace)
        if dict_fields:
            # Nested field cannot contains nothing
            return flask_restplus.fields.Nested(
                namespace.model("_".join(dict_fields), dict_fields),
                required=field.is_required,
                example=field.example(),
                description=field.description,
                enum=field.get_choices(),
                default=field.default_value,
                readonly=field.should_auto_increment,
                skip_none=True,
            )
        else:
            return flask_restplus.fields.Raw(
                required=field.is_required,
                example=field.example(),
                description=field.description,
                enum=field.get_choices(),
                default=field.default_value,
                readonly=field.should_auto_increment,
            )
    elif isinstance(field, ListColumn):
        return flask_restplus.fields.List(
            request_field(field.list_item_column, namespace),
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
        return flask_restplus.fields.List(
            flask_restplus.fields.String,
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
        return flask_restplus.fields.Integer(
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
        return flask_restplus.fields.Float(
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
        return flask_restplus.fields.Boolean(
            required=field.is_required,
            example=field.example(),
            description=field.description,
            enum=field.get_choices(),
            default=field.default_value,
            readonly=field.should_auto_increment,
        )
    elif field.field_type == datetime.date:
        return flask_restplus.fields.Date(
            required=field.is_required,
            example=field.example(),
            description=field.description,
            enum=field.get_choices(),
            default=field.default_value,
            readonly=field.should_auto_increment,
        )
    elif field.field_type == datetime.datetime:
        return flask_restplus.fields.DateTime(
            required=field.is_required,
            example=field.example(),
            description=field.description,
            enum=field.get_choices(),
            default=field.default_value,
            readonly=field.should_auto_increment,
        )
    elif field.field_type == dict:
        return flask_restplus.fields.Raw(
            required=field.is_required,
            example=field.example(),
            description=field.description,
            enum=field.get_choices(),
            default=field.default_value,
            readonly=field.should_auto_increment,
        )
    else:
        return flask_restplus.fields.String(
            required=field.is_required,
            example=field.example(),
            description=field.description,
            enum=field.get_choices(),
            default=field.default_value,
            readonly=field.should_auto_increment,
            min_length=field.min_length,
            max_length=field.max_length,
        )


def get_description_response_fields(collection) -> Dict[str, flask_restplus.fields.Raw]:
    exported_fields = {
        "collection": flask_restplus.fields.String(
            required=True, example="collection", description="Collection name"
        )
    }

    exported_fields.update(
        {
            column.name: flask_restplus.fields.String(
                required=column.is_required,
                example="column",
                description=column.description,
            )
            for column in collection.__dict__.values()
            if isinstance(column, Column)
        }
    )
    return exported_fields
