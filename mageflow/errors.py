class MageflowError(Exception):
    pass


class MissingSignatureError(MageflowError):
    pass


class MissingSwarmItemError(MissingSignatureError):
    pass


class SwarmError(MageflowError):
    pass


class TooManyTasksError(SwarmError, RuntimeError):
    pass


class SwarmIsCanceledError(SwarmError, RuntimeError):
    pass
