from typing import Dict
import flask_restx


def is_mongo_collection(table_or_collection) -> bool:
    """Consider that a class containing __collection_name__ is a Mongo collection."""
    return hasattr(table_or_collection, "__collection_name__")


def add_all_query_fields(
    table_or_collection, is_mongo: bool, parser: flask_restx.reqparse.RequestParser
):
    if is_mongo:
        import layabase._api_mongo

        layabase._api_mongo.add_all_fields(table_or_collection, parser)
    else:
        import layabase._api_sqlalchemy

        layabase._api_sqlalchemy.add_all_fields(table_or_collection, parser)


def add_get_query_fields(
    table_or_collection, parser: flask_restx.reqparse.RequestParser, supports_offset: bool
):
    is_mongo = is_mongo_collection(table_or_collection)
    add_all_query_fields(table_or_collection, is_mongo, parser)
    parser.add_argument("limit", type=flask_restx.inputs.positive, location="args")
    if supports_offset:
        parser.add_argument("offset", type=flask_restx.inputs.natural, location="args")
    if not is_mongo:
        parser.add_argument("order_by", type=str, action="append", location="args")


def add_get_audit_query_fields(
    table_or_collection, history: bool, parser: flask_restx.reqparse.RequestParser, supports_offset: bool
):
    is_mongo = is_mongo_collection(table_or_collection)

    if not history:
        add_all_query_fields(table_or_collection, is_mongo, parser)

    audit_actions = (
        ("Insert", "Update", "Delete", "Rollback") if is_mongo else ("I", "U", "D")
    )
    parser.add_argument(
        "audit_action",
        required=False,
        type=str,
        action="append",
        location="args",
        choices=audit_actions,
    )
    parser.add_argument(
        "audit_date_utc",
        required=False,
        type=flask_restx.inputs.datetime_from_iso8601,
        action="append",
        location="args",
    )
    parser.add_argument(
        "audit_user", required=False, type=str, action="append", location="args"
    )
    parser.add_argument(
        "revision", required=False, type=int, action="append", location="args"
    )

    parser.add_argument("limit", type=flask_restx.inputs.positive, location="args")
    if supports_offset:
        parser.add_argument("offset", type=flask_restx.inputs.natural, location="args")
    if not is_mongo:
        parser.add_argument("order_by", type=str, action="append", location="args")


def add_delete_query_fields(
    table_or_collection, parser: flask_restx.reqparse.RequestParser
):
    add_all_query_fields(
        table_or_collection, is_mongo_collection(table_or_collection), parser
    )


def add_rollback_query_fields(
    table_or_collection, parser: flask_restx.reqparse.RequestParser
):
    add_all_query_fields(
        table_or_collection, is_mongo_collection(table_or_collection), parser
    )
    parser.add_argument("revision", type=flask_restx.inputs.positive, required=True)


def add_history_query_fields(
    table_or_collection, parser: flask_restx.reqparse.RequestParser, supports_offset: bool
):
    parser.add_argument(
        "valid_since_revision",
        required=False,
        type=int,
        action="append",
        location="args",
    )
    parser.add_argument(
        "valid_until_revision",
        required=False,
        type=int,
        action="append",
        location="args",
    )

    add_all_query_fields(
        table_or_collection, is_mongo_collection(table_or_collection), parser
    )

    parser.add_argument("limit", type=flask_restx.inputs.positive)
    if supports_offset:
        parser.add_argument("offset", type=flask_restx.inputs.natural)


def all_request_fields(
    table_or_collection, is_mongo: bool, namespace: flask_restx.Namespace
) -> Dict[str, flask_restx.fields.Raw]:
    if is_mongo:
        import layabase._api_mongo

        return layabase._api_mongo.all_request_fields(table_or_collection, namespace)
    else:
        import layabase._api_sqlalchemy

        return layabase._api_sqlalchemy.all_request_fields(table_or_collection)


def get_response_fields(
    table_or_collection, namespace: flask_restx.Namespace
) -> Dict[str, flask_restx.fields.Raw]:
    return all_request_fields(
        table_or_collection, is_mongo_collection(table_or_collection), namespace
    )


def get_history_response_fields(
    table_or_collection, namespace: flask_restx.Namespace
) -> Dict[str, flask_restx.fields.Raw]:
    fields = {
        "valid_since_revision": flask_restx.fields.Integer(
            example=1,
            description="Record is valid since this revision (included).",
            readonly=False,
        ),
        "valid_until_revision": flask_restx.fields.Integer(
            example=1,
            description="Record is valid until this revision (excluded).",
            readonly=False,
        ),
    }
    fields.update(
        all_request_fields(
            table_or_collection, is_mongo_collection(table_or_collection), namespace
        )
    )
    return fields


def get_audit_response_fields(
    table_or_collection, history: bool, namespace: flask_restx.Namespace
) -> Dict[str, flask_restx.fields.Raw]:
    is_mongo = is_mongo_collection(table_or_collection)
    if not history:
        fields = all_request_fields(table_or_collection, is_mongo, namespace)
    else:
        fields = {}

    fields["audit_action"] = flask_restx.fields.String(
        example="Insert" if is_mongo else "I",
        enum=("Insert", "Update", "Delete", "Rollback")
        if is_mongo
        else ("I", "U", "D"),
        readonly=False,
    )
    fields["audit_date_utc"] = flask_restx.fields.DateTime(
        example="2017-09-24T15:36:09", readonly=False
    )
    fields["audit_user"] = flask_restx.fields.String(
        example="sample audit_user", readonly=False
    )
    fields["revision"] = flask_restx.fields.Integer(example=1, readonly=True)

    return fields


def get_description_response_fields(
    table_or_collection,
) -> Dict[str, flask_restx.fields.Raw]:
    if is_mongo_collection(table_or_collection):
        import layabase._api_mongo

        return layabase._api_mongo.get_description_response_fields(table_or_collection)
    else:
        import layabase._api_sqlalchemy

        return layabase._api_sqlalchemy.get_description_response_fields(
            table_or_collection
        )


def post_request_fields(
    table_or_collection, namespace: flask_restx.Namespace
) -> Dict[str, flask_restx.fields.Raw]:
    return all_request_fields(
        table_or_collection, is_mongo_collection(table_or_collection), namespace
    )


def put_request_fields(
    table_or_collection, namespace: flask_restx.Namespace
) -> Dict[str, flask_restx.fields.Raw]:
    return all_request_fields(
        table_or_collection, is_mongo_collection(table_or_collection), namespace
    )
