from flask_restplus import fields
from marshmallow_sqlalchemy.fields import fields as marshmallow_fields


def get_type(marshmallow_field):
    """
    Return the Flask RestPlus field type corresponding to this SQL Alchemy Marshmallow field.

    :raises Exception if field type is not managed yet.
    """
    if isinstance(marshmallow_field, marshmallow_fields.String):
        return fields.String(required=marshmallow_field.required, example='sample_value')
    if isinstance(marshmallow_field, marshmallow_fields.Integer):
        return fields.Integer(required=marshmallow_field.required, example='0')
    if isinstance(marshmallow_field, marshmallow_fields.Boolean):
        return fields.Boolean(required=marshmallow_field.required, example='true')
    if isinstance(marshmallow_field, marshmallow_fields.Date):
        return fields.Date(required=marshmallow_field.required, example='2017-09-24')
    if isinstance(marshmallow_field, marshmallow_fields.DateTime):
        return fields.DateTime(required=marshmallow_field.required, example='2017-09-24T15:36:09')
    if isinstance(marshmallow_field, marshmallow_fields.Decimal):
        return fields.Decimal(required=marshmallow_field.required, example='0.0')
    if isinstance(marshmallow_field, marshmallow_fields.Float):
        return fields.Float(required=marshmallow_field.required, example='0.0')
    if isinstance(marshmallow_field, marshmallow_fields.Number):
        return fields.Decimal(required=marshmallow_field.required, example='0.0')
    if isinstance(marshmallow_field, marshmallow_fields.Time):
        return fields.DateTime(required=marshmallow_field.required, example='15:36:09')

    raise Exception('Field type cannot be guessed for {0} field.'.format(marshmallow_field))


def all_schema_fields(sql_alchemy_class, api):
    """
    Flask RestPlus Model describing a SQL Alchemy class using schema fields.
    """
    exported_fields = {
        field.name: get_type(field)
        for field in sql_alchemy_class.schema().fields.values()
    }
    return api.model(sql_alchemy_class.__name__, exported_fields)


def model_description(sql_alchemy_class, api):
    """
    Flask RestPlus Model describing a SQL Alchemy model.
    """
    exported_fields = {
        'table': fields.String(required=True, example='table'),
        'schema': fields.String(required=True, example='schema')
    }
    exported_fields.update({
        field.name: fields.String(example='column')
        for field in sql_alchemy_class.schema().fields.values()
    })
    return api.model(''.join([sql_alchemy_class.__name__, 'Description']), exported_fields)
