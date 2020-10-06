from typing import Union, List, Dict


class ControllerModelNotSet(Exception):
    def __init__(self, controller):
        Exception.__init__(
            self,
            f"layabase.load must be called with this {controller.__class__.__name__} instance before using any provided CRUDController feature.",
        )


class MultiSchemaNotSupported(Exception):
    def __init__(self):
        Exception.__init__(self, "SQLite does not manage multi-schemas..")


class ValidationFailed(Exception):
    def __init__(
        self, received_data: Union[List, Dict], errors: Dict = None, message: str = ""
    ):
        """
        Represent a client data validation error.

        :param received_data: Data triggering the error. Should be a list or a dictionary in most cases.
        :param errors: To be used if a specific field triggered the error.
        If received_data is a list:
            key is supposed to be the index in received_data
            value is supposed to be a the same as if received_data was the dictionary at this index
        If received_data is a dict:
            key is supposed to be the field name in error
            value is supposed to be a list of error messages on this field
        :param message: The error message in case errors cannot be provided.
        """
        self.received_data = received_data
        self.errors = errors if errors else {"": [message]}


class DatabaseError(Exception):
    def __init__(self, original_exception: Exception):
        Exception.__init__(
            self, f"A error occurred while querying database: {str(original_exception)}"
        )
