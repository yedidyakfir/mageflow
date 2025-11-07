class OrchestratorError(Exception):
    pass


class MissingSignatureError(OrchestratorError):
    pass


class MissingSwarmItemError(MissingSignatureError):
    pass
