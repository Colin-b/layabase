class ControllerModelNotSet(Exception):
    def __init__(self, controller_class):
        Exception.__init__(
            self,
            f"{controller_class.__name__}.model must be set before calling layabase.load function.",
        )
