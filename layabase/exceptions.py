class ControllerModelNotSet(Exception):
    def __init__(self, controller):
        Exception.__init__(
            self,
            f"layabase.load must be called with this {controller.__class__.__name__} instance before using any provided CRUDController feature.",
        )
