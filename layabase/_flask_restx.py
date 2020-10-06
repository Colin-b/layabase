import flask_restx
from layabase._api import add_get_query_fields, add_delete_query_fields, add_rollback_query_fields, \
    add_history_query_fields, add_get_audit_query_fields, post_request_fields, put_request_fields, get_response_fields, \
    get_history_response_fields, get_audit_response_fields, get_description_response_fields


class ParsersAndModels:
    def __init__(self, table_or_collection, history: bool, audit: bool, supports_offset: bool):
        self.table_or_collection = table_or_collection
        self.history = history
        self.audit = audit

        self.query_get_parser = flask_restx.reqparse.RequestParser()
        add_get_query_fields(table_or_collection, self.query_get_parser, supports_offset)

        self.query_delete_parser = flask_restx.reqparse.RequestParser()
        add_delete_query_fields(table_or_collection, self.query_delete_parser)

        self.query_rollback_parser = flask_restx.reqparse.RequestParser()
        self.query_get_history_parser = flask_restx.reqparse.RequestParser()
        if history:
            add_rollback_query_fields(table_or_collection, self.query_rollback_parser)
            add_history_query_fields(table_or_collection, self.query_get_history_parser, supports_offset)

        self.query_get_audit_parser = flask_restx.reqparse.RequestParser()
        if audit:
            add_get_audit_query_fields(
                table_or_collection, history, self.query_get_audit_parser, supports_offset
            )

        # CRUD model definition (instead of request parsers)
        self.json_post_model = None
        self.json_put_model = None

        # CRUD response marshallers
        self.get_response_model = None
        self.get_history_response_model = None
        self.get_audit_response_model = None
        self.get_model_description_response_model = None

    def init_models(self, namespace: flask_restx.Namespace):
        """
        Create flask-restx models that can be used to marshall results (and document service).

        :param namespace: Flask-RestX API or namespace.
        """
        self.json_post_model = namespace.model(
            f"{self.table_or_collection.__name__}_PostRequestModel",
            post_request_fields(self.table_or_collection, namespace),
        )
        self.json_put_model = namespace.model(
            f"{self.table_or_collection.__name__}_PutRequestModel",
            put_request_fields(self.table_or_collection, namespace),
        )
        self.get_response_model = namespace.model(
            f"{self.table_or_collection.__name__}_GetResponseModel",
            get_response_fields(self.table_or_collection, namespace),
        )
        self.get_history_response_model = namespace.model(
            f"{self.table_or_collection.__name__}_GetHistoryResponseModel",
            get_history_response_fields(self.table_or_collection, namespace),
        )
        if self.audit:
            self.get_audit_response_model = namespace.model(
                f"{self.table_or_collection.__name__}_GetAuditResponseModel",
                get_audit_response_fields(
                    self.table_or_collection, self.history, namespace
                ),
            )
        self.get_model_description_response_model = namespace.model(
            f"{self.table_or_collection.__name__}_GetDescriptionResponseModel",
            get_description_response_fields(self.table_or_collection),
        )
