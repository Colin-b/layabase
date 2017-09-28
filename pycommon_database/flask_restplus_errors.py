from flask_restplus import fields


class ValidationFailed(Exception):
    def __init__(self, marshmallow_errors):
        self.errors = marshmallow_errors


def _failed_validation_model(api):
    exception_details = {
        'field_name': fields.String(description='Name of the field that could not be validated.',
                                    required=True,
                                    example='sample_field_name'),
        'message': fields.String(description='Reason why the validation failed.',
                                 required=True,
                                 example='This is the reason why this field was not validated.'),
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
        error_list = []
        for field, message in failed_validation.errors:
            error_list.append({
                'field_name': field,
                'message': message,
            })
        return error_list, 400

    return 400, 'Validation failed.', exception_model
