import flask_restplus


def is_mongo_collection(table_or_collection) -> bool:
    """Consider that a class containing __collection_name__ is a Mongo collection."""
    return hasattr(table_or_collection, "__collection_name__")


def add_all_fields(
    table_or_collection, is_mongo: bool, parser: flask_restplus.reqparse.RequestParser
):
    if is_mongo:
        import layabase._api_mongo

        layabase._api_mongo.add_all_fields(table_or_collection, parser)
    else:
        import layabase._api_sqlalchemy

        layabase._api_sqlalchemy.add_all_fields(table_or_collection, parser)


def add_get_query_fields(
    table_or_collection, parser: flask_restplus.reqparse.RequestParser
):
    is_mongo = is_mongo_collection(table_or_collection)
    add_all_fields(table_or_collection, is_mongo, parser)
    parser.add_argument("limit", type=flask_restplus.inputs.positive, location="args")
    parser.add_argument("offset", type=flask_restplus.inputs.natural, location="args")
    if not is_mongo:
        parser.add_argument("order_by", type=str, action="append", location="args")


def add_get_audit_query_fields(
    table_or_collection, history: bool, parser: flask_restplus.reqparse.RequestParser
):
    is_mongo = is_mongo_collection(table_or_collection)

    if not history:
        add_all_fields(table_or_collection, is_mongo, parser)

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
        type=flask_restplus.inputs.datetime_from_iso8601,
        action="append",
        location="args",
    )
    parser.add_argument(
        "audit_user", required=False, type=str, action="append", location="args"
    )
    parser.add_argument(
        "revision", required=False, type=int, action="append", location="args"
    )

    parser.add_argument("limit", type=flask_restplus.inputs.positive, location="args")
    parser.add_argument("offset", type=flask_restplus.inputs.natural, location="args")
    if not is_mongo:
        parser.add_argument("order_by", type=str, action="append", location="args")


def add_delete_query_fields(
    table_or_collection, parser: flask_restplus.reqparse.RequestParser
):
    add_all_fields(
        table_or_collection, is_mongo_collection(table_or_collection), parser
    )


def add_rollback_query_fields(
    table_or_collection, parser: flask_restplus.reqparse.RequestParser
):
    add_all_fields(
        table_or_collection, is_mongo_collection(table_or_collection), parser
    )
    parser.add_argument("revision", type=flask_restplus.inputs.positive, required=True)


def add_history_query_fields(
    table_or_collection, parser: flask_restplus.reqparse.RequestParser
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

    add_all_fields(
        table_or_collection, is_mongo_collection(table_or_collection), parser
    )

    parser.add_argument("limit", type=flask_restplus.inputs.positive)
    parser.add_argument("offset", type=flask_restplus.inputs.natural)
