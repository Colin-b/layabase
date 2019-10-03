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
