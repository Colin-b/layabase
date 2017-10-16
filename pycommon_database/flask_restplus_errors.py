import logging
from flask_restplus import fields

logger = logging.getLogger(__name__)


class ValidationFailed(Exception):
    def __init__(self, received_data, marshmallow_errors=None, message=''):
        self.received_data = received_data
        self.errors = marshmallow_errors if marshmallow_errors else {'': [message]}


class ModelCouldNotBeFound(Exception):
    def __init__(self, requested_data):
        self.requested_data = requested_data


def _failed_field_validation_model(api):
    exception_details = {
        'field_name': fields.String(description='Name of the field that could not be validated.',
                                    required=True,
                                    example='sample_field_name'),
        'messages': fields.List(fields.String(description='Reason why the validation failed.',
                                              required=True,
                                              example='This is the reason why this field was not validated.')
                                ),
    }
    return api.model('FieldValidationFailed', exception_details)


def _failed_validation_model(api):
    exception_details = {
        'fields': fields.List(fields.Nested(_failed_field_validation_model(api))),
    }
    return api.model('ValidationFailed', exception_details)


def add_failed_validation_handler(api):
    """
    Add the default ValidationFailed handler.

    :param api: The root Api
    """
    exception_model = _failed_validation_model(api)

    @api.errorhandler(ValidationFailed)
    @api.marshal_with(exception_model, code=400)
    def handle_exception(failed_validation):
        """This is the default validation error handling."""
        logger.exception('Validation failed.')
        error_list = []
        for field, messages in failed_validation.errors.items():
            error_list.append({
                'field_name': field,
                'messages': messages,
            })
        return {'fields': error_list}, 400

    return 400, 'Validation failed.', exception_model


def _model_could_not_be_found_model(api):
    exception_details = {
        'message': fields.String(description='Description of the error.',
                                 required=True,
                                 example='Corresponding model could not be found.'),
    }
    return api.model('ModelCouldNotBeFound', exception_details)


def add_model_could_not_be_found_handler(api):
    """
    Add the default ModelCouldNotBeFound handler.

    :param api: The root Api
    """
    exception_model = _model_could_not_be_found_model(api)

    @api.errorhandler(ModelCouldNotBeFound)
    @api.marshal_with(exception_model, code=404)
    def handle_exception(model_could_not_be_found):
        """This is the default model could not be found handling."""
        logger.exception('Corresponding model could not be found.')
        return {'message': 'Corresponding model could not be found.'}, 404

    return 404, 'Corresponding model could not be found.', exception_model
