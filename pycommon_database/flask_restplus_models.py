import datetime
from flask_restplus import fields, reqparse
from marshmallow_sqlalchemy.fields import fields as marshmallow_fields
import sqlalchemy


def get_rest_plus_type(marshmallow_field):
    """
    Return the Flask RestPlus field type (as a class) corresponding to this SQL Alchemy Marshmallow field.

    :raises Exception if field type is not managed yet.
    """
    if isinstance(marshmallow_field, marshmallow_fields.String):
        return fields.String
    if isinstance(marshmallow_field, marshmallow_fields.Integer):
        return fields.Integer
    if isinstance(marshmallow_field, marshmallow_fields.Boolean):
        return fields.Boolean
    if isinstance(marshmallow_field, marshmallow_fields.Date):
        return fields.Date
    if isinstance(marshmallow_field, marshmallow_fields.DateTime):
        return fields.DateTime
    if isinstance(marshmallow_field, marshmallow_fields.Decimal):
        return fields.Fixed
    if isinstance(marshmallow_field, marshmallow_fields.Float):
        return fields.Float
    if isinstance(marshmallow_field, marshmallow_fields.Number):
        return fields.Decimal
    if isinstance(marshmallow_field, marshmallow_fields.Time):
        return fields.DateTime
    if isinstance(marshmallow_field, marshmallow_fields.Field):
        return fields.String

    raise Exception('Flask RestPlus field type cannot be guessed for {0} field.'.format(marshmallow_field))


def get_example(marshmallow_field):
    """
    Return an Example value corresponding to this SQL Alchemy Marshmallow field.
    """
    if isinstance(marshmallow_field, marshmallow_fields.Integer):
        return '0'
    if isinstance(marshmallow_field, marshmallow_fields.Boolean):
        return 'true'
    if isinstance(marshmallow_field, marshmallow_fields.Date):
        return '2017-09-24'
    if isinstance(marshmallow_field, marshmallow_fields.DateTime):
        return '2017-09-24T15:36:09'
    if isinstance(marshmallow_field, marshmallow_fields.Decimal):
        return '0.0'
    if isinstance(marshmallow_field, marshmallow_fields.Float):
        return '0.0'
    if isinstance(marshmallow_field, marshmallow_fields.Number):
        return '0.0'
    if isinstance(marshmallow_field, marshmallow_fields.Time):
        return '15:36:09'
    if isinstance(marshmallow_field, marshmallow_fields.List):
        return 'xxxx'

    return 'sample_value'


def get_python_type(sql_alchemy_field):
    """
    Return the Python type corresponding to this SQL Alchemy field.

    :raises Exception if field type is not managed yet.
    """
    if not hasattr(sql_alchemy_field, 'columns'):
        raise Exception(f'Python field type cannot be guessed for {sql_alchemy_field}.')

    if len(sql_alchemy_field.columns) != 1:
        raise Exception(f'Python field type cannot be guessed for {sql_alchemy_field} field. '
                        f'Only one column should be provided instead of: {sql_alchemy_field.columns}.')

    column_type = sql_alchemy_field.columns[0].type

    if isinstance(column_type, sqlalchemy.String):
        return str
    if isinstance(column_type, sqlalchemy.Integer):
        return int
    if isinstance(column_type, sqlalchemy.Boolean):
        return bool
    if isinstance(column_type, sqlalchemy.Date):
        return datetime.date
    if isinstance(column_type, sqlalchemy.DateTime):
        return datetime.datetime
    if isinstance(column_type, sqlalchemy.Time):
        return datetime.time
    if isinstance(column_type, sqlalchemy.Float):
        return float
    if isinstance(column_type, sqlalchemy.Enum):
        return list
    if isinstance(column_type, sqlalchemy.Numeric):
        return float

    raise Exception(f'Python field type cannot be guessed for {sql_alchemy_field} field ({column_type}).')


def all_schema_fields(sql_alchemy_class, api):
    """
    Flask RestPlus Model describing a SQL Alchemy class using schema fields.
    """
    exported_fields = {
        field.name: get_rest_plus_type(field)(required=field.required, example='sample_value')
        for field in sql_alchemy_class.schema().fields.values()
    }
    return api.model(sql_alchemy_class.__name__, exported_fields)


def model_description(sql_alchemy_class, api):
    """
    Flask RestPlus Model describing a SQL Alchemy model.
    """
    exported_fields = {
        'table': fields.String(required=True, example='table'),
    }

    if hasattr(sql_alchemy_class, 'table_args__'):
        exported_fields['schema'] = fields.String(required=True, example='schema')

    exported_fields.update({
        field.name: fields.String(example='column')
        for field in sql_alchemy_class.schema().fields.values()
    })

    return api.model(''.join([sql_alchemy_class.__name__, 'Description']), exported_fields)


def all_model_fields(sql_alchemy_class):
    request_parser = reqparse.RequestParser()

    mapper = sqlalchemy.inspect(sql_alchemy_class)
    for column in mapper.attrs:
        request_parser.add_argument(column.key, type=get_python_type(column))

    return request_parser
