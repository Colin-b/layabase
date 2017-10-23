import datetime
from flask_restplus import fields, reqparse, inputs
from marshmallow_sqlalchemy.fields import fields as marshmallow_fields
from marshmallow import validate


def get_rest_plus_type(marshmallow_field):
    """
    Return the Flask RestPlus field type (as a class) corresponding to this SQL Alchemy Marshmallow field.

    :raises Exception if field type is not managed yet.
    """
    if isinstance(marshmallow_field, marshmallow_fields.String):
        return fields.String
    if isinstance(marshmallow_field, marshmallow_fields.Integer):
        return fields.Integer
    if isinstance(marshmallow_field, marshmallow_fields.Decimal):
        return fields.Fixed
    if isinstance(marshmallow_field, marshmallow_fields.Float):
        return fields.Float
    if isinstance(marshmallow_field, marshmallow_fields.Number):
        return fields.Decimal
    if isinstance(marshmallow_field, marshmallow_fields.Boolean):
        return fields.Boolean
    if isinstance(marshmallow_field, marshmallow_fields.Date):
        return fields.Date
    if isinstance(marshmallow_field, marshmallow_fields.DateTime):
        return fields.DateTime
    if isinstance(marshmallow_field, marshmallow_fields.Time):
        return fields.DateTime
    # SQLAlchemy Enum fields will be converted to Marshmallow Raw Field
    if isinstance(marshmallow_field, marshmallow_fields.Field):
        return fields.String

    raise Exception(f'Flask RestPlus field type cannot be guessed for {marshmallow_field} field.')


def get_example(marshmallow_field):
    default_value = _get_default_value(marshmallow_field)
    if default_value:
        return str(default_value)

    choices = get_choices(marshmallow_field)
    return str(choices[0]) if choices else get_default_example(marshmallow_field)


def get_choices(marshmallow_field):
    if marshmallow_field:
        for validator in marshmallow_field.validators:
            if isinstance(validator, validate.OneOf):
                return validator.choices


def _get_default_value(marshmallow_field):
    return marshmallow_field.metadata.get('sqlalchemy_default', None) if marshmallow_field else None


def get_default_example(marshmallow_field):
    """
    Return an Example value corresponding to this SQL Alchemy Marshmallow field.
    """
    if isinstance(marshmallow_field, marshmallow_fields.Integer):
        return '0'
    if isinstance(marshmallow_field, marshmallow_fields.Number):
        return '0.0'
    if isinstance(marshmallow_field, marshmallow_fields.Boolean):
        return 'true'
    if isinstance(marshmallow_field, marshmallow_fields.Date):
        return '2017-09-24'
    if isinstance(marshmallow_field, marshmallow_fields.DateTime):
        return '2017-09-24T15:36:09'
    if isinstance(marshmallow_field, marshmallow_fields.Time):
        return '15:36:09'
    if isinstance(marshmallow_field, marshmallow_fields.List):
        return 'xxxx'

    return 'sample_value'


def _is_json_string(marshmallow_field):
    """
    Return True if the field should be sent as a string within JSON.
    """
    if isinstance(marshmallow_field, marshmallow_fields.Integer) or \
            isinstance(marshmallow_field, marshmallow_fields.Boolean) or \
            isinstance(marshmallow_field, marshmallow_fields.Number):
        return False

    return True


def get_python_type(marshmallow_field):
    """
    Return the Python type corresponding to this SQL Alchemy Marshmallow field.

    :raises Exception if field type is not managed yet.
    """
    if isinstance(marshmallow_field, marshmallow_fields.String):
        return str
    if isinstance(marshmallow_field, marshmallow_fields.Integer):
        return int
    if isinstance(marshmallow_field, marshmallow_fields.Number):
        return float
    if isinstance(marshmallow_field, marshmallow_fields.Boolean):
        return inputs.boolean
    if isinstance(marshmallow_field, marshmallow_fields.Date):
        return inputs.date_from_iso8601
    if isinstance(marshmallow_field, marshmallow_fields.DateTime):
        return inputs.datetime_from_iso8601
    if isinstance(marshmallow_field, marshmallow_fields.List):
        return list
    # SQLAlchemy Enum fields will be converted to Marshmallow Raw Field
    if isinstance(marshmallow_field, marshmallow_fields.Field):
        return str

    raise Exception(f'Python field type cannot be guessed for {marshmallow_field} field.')


def model_with_fields(api, name: str, marshmallow_fields_list):
    """
    Flask RestPlus Model describing a SQL Alchemy class using schema fields.
    """
    exported_fields = {
        field.name: get_rest_plus_type(field)(
            required=field.required,
            example=str(get_example(field)),
            description=field.metadata.get('description', None),
            enum=get_choices(field),
            default=_get_default_value(field),
        )
        for field in marshmallow_fields_list
    }
    return api.model(name, exported_fields)


def model_describing_sql_alchemy_mapping(api, sql_alchemy_class):
    """
    Flask RestPlus Model describing a SQL Alchemy model.
    """
    exported_fields = {
        'table': fields.String(required=True, example='table', description='Table name'),
    }

    if hasattr(sql_alchemy_class, 'table_args__'):
        exported_fields['schema'] = fields.String(required=True, example='schema', description='Table schema')

    exported_fields.update({
        field.name: fields.String(
            required=field.required,
            example='column',
            description=field.metadata.get('description', None),
        )
        for field in sql_alchemy_class.schema().fields.values()
    })

    return api.model(''.join([sql_alchemy_class.__name__, 'Description']), exported_fields)


def query_parser_with_fields(marshmallow_fields_list):
    query_parser = reqparse.RequestParser()
    for field in marshmallow_fields_list:
        query_parser.add_argument(
            field.name,
            type=get_python_type(field),
        )
    return query_parser
