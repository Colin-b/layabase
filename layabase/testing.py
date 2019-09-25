def reset(base):
    """
    If the database was already created, then drop all tables and recreate them all.

    :param base: database object as returned by the load function (Mandatory).
    """
    if hasattr(base, "is_mongos"):
        import layabase.database_mongo as database_mongo

        database_mongo._reset(base)
    else:
        import layabase.database_sqlalchemy as database_sqlalchemy

        database_sqlalchemy._reset(base)
