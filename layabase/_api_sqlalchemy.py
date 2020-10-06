from typing import Dict, Type

import flask_restx
import sqlalchemy

from layabase import ComparisonSigns


def add_all_fields(table, parser):
    for name, column in table.__dict__.items():
        if isinstance(column, sqlalchemy.Column):
            _add_query_field(parser, name, column)


def _add_query_field(
    parser: flask_restx.reqparse.RequestParser, name: str, column: sqlalchemy.Column
):
    parser.add_argument(
        name,
        required=column.info.get("layabase", {}).get("required_on_query", False),
        type=_get_parser_type(column),
        action="append",
        location="args",
        choices=_get_choices(column),
    )


def _get_parser_type(column: sqlalchemy.Column) -> callable:
    """
    Return a function taking a single parameter (the value) and converting to the required field type.
    """
    column_type = column.type
    allow_comparison_signs = column.info.get("layabase", {}).get(
        "allow_comparison_signs", False
    )
    if isinstance(column_type, sqlalchemy.String) or isinstance(
        column_type, sqlalchemy.Enum
    ):
        return str
    if isinstance(column_type, sqlalchemy.Integer):
        return _validate_int if allow_comparison_signs else int
    if isinstance(column_type, sqlalchemy.Numeric):
        return _validate_float if allow_comparison_signs else float
    if isinstance(column_type, sqlalchemy.Boolean):
        return flask_restx.inputs.boolean
    if isinstance(column_type, sqlalchemy.Date):
        return (
            _validate_date
            if allow_comparison_signs
            else flask_restx.inputs.date_from_iso8601
        )
    if isinstance(column_type, sqlalchemy.DateTime):
        return (
            _validate_date_time
            if allow_comparison_signs
            else flask_restx.inputs.datetime_from_iso8601
        )

    # Default to str for unhandled types
    return str


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
            return value[0], flask_restx.inputs.datetime_from_iso8601(value[1])

    return flask_restx.inputs.datetime_from_iso8601(value)


_validate_date_time.__schema__ = {"type": "string"}


def _validate_date(value):
    if isinstance(value, str):
        value = ComparisonSigns.deserialize(value)

        # When using comparison signs, the value is a tuple containing the comparison sign and the value.
        # ex: (ComparisonSigns.Lower, 124)
        if isinstance(value, tuple):
            return value[0], flask_restx.inputs.date_from_iso8601(value[1])

    return flask_restx.inputs.date_from_iso8601(value)


_validate_date.__schema__ = {"type": "string"}


def all_request_fields(table) -> Dict[str, flask_restx.fields.Raw]:
    return {
        name: request_field(column)
        for name, column in table.__dict__.items()
        if isinstance(column, sqlalchemy.Column)
    }


def request_field(column: sqlalchemy.Column) -> flask_restx.fields.Raw:
    return request_field_type(column.type)(
        required=(column.primary_key or column.nullable is False)
        and column.autoincrement is not True,
        example=_get_example(column),
        description=column.doc,
        enum=_get_choices(column),
        default=column.default.arg if column.default else None,
        # Compare to True because value might be set to "auto" str
        readonly=column.autoincrement is True,
    )


def request_field_type(column_type) -> Type[flask_restx.fields.Raw]:
    """
    Return the Flask RestPlus field type (as a class) corresponding to this SQL Alchemy Marshmallow field.
    Default to String field.
    TODO Faster to use a dict from type to field ?
    """
    if isinstance(column_type, sqlalchemy.Integer):
        return flask_restx.fields.Integer
    if isinstance(column_type, sqlalchemy.Numeric):
        return flask_restx.fields.Float
    if isinstance(column_type, sqlalchemy.Boolean):
        return flask_restx.fields.Boolean
    if isinstance(column_type, sqlalchemy.Date):
        return flask_restx.fields.Date
    if isinstance(column_type, sqlalchemy.DateTime):
        return flask_restx.fields.DateTime
    if isinstance(column_type, sqlalchemy.Time):
        return flask_restx.fields.DateTime

    return flask_restx.fields.String


def _get_example(column: sqlalchemy.Column) -> str:
    default_value = column.default.arg if column.default else None
    if default_value:
        return str(default_value)

    choices = _get_choices(column)
    return str(choices[0]) if choices else _get_default_example(column.type)


def _get_choices(column: sqlalchemy.Column):
    if isinstance(column.type, sqlalchemy.Enum):
        return column.type.enums


def _get_default_example(column_type):
    """
    Return an Example value corresponding to this SQLAlchemy field type.
    """
    if isinstance(column_type, sqlalchemy.Integer):
        return 1
    if isinstance(column_type, sqlalchemy.Numeric):
        return 1.4
    if isinstance(column_type, sqlalchemy.Boolean):
        return True
    if isinstance(column_type, sqlalchemy.Date):
        return "2017-09-24"
    if isinstance(column_type, sqlalchemy.DateTime):
        return "2017-09-24T15:36:09"
    if isinstance(column_type, sqlalchemy.Time):
        return "15:36:09"

    return "sample_value"


def get_description_response_fields(table) -> Dict[str, flask_restx.fields.Raw]:
    fields = {
        "table": flask_restx.fields.String(
            required=True, example="table", description="Table name"
        )
    }

    if "__table_args__" in table.__dict__:
        fields["schema"] = flask_restx.fields.String(
            required=True, example="schema", description="Table schema"
        )

    fields.update(
        {
            name: flask_restx.fields.String(
                required=(column.primary_key or column.nullable is False)
                and column.autoincrement is not True,
                example="column",
                description=column.doc,
            )
            for name, column in table.__dict__.items()
            if isinstance(column, sqlalchemy.Column)
        }
    )
    return fields
