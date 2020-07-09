from typing import Any


class ClientError(Exception):
    def __init__(self, status_code: int, detail: Any):
        self.status_code = status_code
        self.detail = detail


class ServerError(Exception):
    def __init__(self, status_code: int, detail: Any):
        self.status_code = status_code
        self.detail = detail


class TooManyRetriesError(RecursionError):
    def __init__(self, message: str, detail: str):
        self.message = message
        self.detail = detail


class RecordNotFoundError(Exception):
    pass


class RecordAlreadyExistsError(Exception):
    pass


class BadRequestError(Exception):
    pass
