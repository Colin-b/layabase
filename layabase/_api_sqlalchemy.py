from typing import Dict, Type

import flask_restplus
import sqlalchemy


def add_all_fields(table, parser):
    for name, column in table.__dict__.items():
        if isinstance(column, sqlalchemy.Column):
            _add_query_field(parser, name, column)


def _add_query_field(
    parser: flask_restplus.reqparse.RequestParser, name: str, column: sqlalchemy.Column
):
    parser.add_argument(
        name,
        required=column.info.get("marshmallow", {}).get("required_on_query", False),
        type=_get_parser_type(column.type),
        action="append",
        location="args",
    )


def _get_parser_type(column_type):
    """
    Return the Python type corresponding to this SQL Alchemy Marshmallow field.

    Default to str,
    """
    if isinstance(column_type, sqlalchemy.String) or isinstance(
        column_type, sqlalchemy.Enum
    ):
        return str
    if isinstance(column_type, sqlalchemy.Integer):
        return int
    if isinstance(column_type, sqlalchemy.Numeric):
        return float
    if isinstance(column_type, sqlalchemy.Boolean):
        return flask_restplus.inputs.boolean
    if isinstance(column_type, sqlalchemy.Date):
        return flask_restplus.inputs.date_from_iso8601
    if isinstance(column_type, sqlalchemy.DateTime):
        return flask_restplus.inputs.datetime_from_iso8601

    # Default to str for unhandled types
    return str


def all_request_fields(table) -> Dict[str, flask_restplus.fields.Raw]:
    return {
        name: request_field(column)
        for name, column in table.__dict__.items()
        if isinstance(column, sqlalchemy.Column)
    }


def request_field(column: sqlalchemy.Column) -> flask_restplus.fields.Raw:
    return request_field_type(column.type)(
        required=column.primary_key or column.nullable is False,
        example=_get_example(column),
        description=column.description,
        enum=_get_choices(column),
        default=column.default.arg if column.default else None,
        # Compare to True because value might be set to "auto" str
        readonly=column.autoincrement is True,
    )


def request_field_type(column_type) -> Type[flask_restplus.fields.Raw]:
    """
    Return the Flask RestPlus field type (as a class) corresponding to this SQL Alchemy Marshmallow field.
    Default to String field.
    TODO Faster to use a dict from type to field ?
    """
    if isinstance(column_type, sqlalchemy.String):
        return flask_restplus.fields.String
    if isinstance(column_type, sqlalchemy.Integer):
        return flask_restplus.fields.Integer
    if isinstance(column_type, sqlalchemy.Numeric):
        return flask_restplus.fields.Float
    if isinstance(column_type, sqlalchemy.Boolean):
        return flask_restplus.fields.Boolean
    if isinstance(column_type, sqlalchemy.Date):
        return flask_restplus.fields.Date
    if isinstance(column_type, sqlalchemy.DateTime):
        return flask_restplus.fields.DateTime
    if isinstance(column_type, sqlalchemy.Time):
        return flask_restplus.fields.DateTime

    # SQLAlchemy Enum fields will be converted to Marshmallow Raw Field
    return flask_restplus.fields.String


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


def get_description_response_fields(table) -> Dict[str, flask_restplus.fields.Raw]:
    fields = {
        "table": flask_restplus.fields.String(
            required=True, example="table", description="Table name"
        )
    }

    if "__table_args__" in table.__dict__:
        fields["schema"] = flask_restplus.fields.String(
            required=True, example="schema", description="Table schema"
        )

    fields.update(
        {
            name: flask_restplus.fields.String(
                required=column.primary_key or column.nullable is False,
                example="column",
                description=column.description,
            )
            for name, column in table.__dict__.items()
            if isinstance(column, sqlalchemy.Column)
        }
    )
    return fields
