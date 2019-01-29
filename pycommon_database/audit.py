def current_user_name(anonymous_user_name: str = "") -> str:
    """
    TODO Should we comply with GDPR? Should we avoid storing those data in plain text?
    Provide the name of the current user performing a request (if any).
    :param anonymous_user_name: Default value in case there is no known user performing a request.
    """
    try:
        import flask

        if flask.has_request_context() and hasattr(flask.g, "current_user"):
            return flask.g.current_user.name
        return anonymous_user_name
    except ImportError:
        return anonymous_user_name  # Ensure fail safe call
