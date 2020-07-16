import sys
import traceback

from fastapi import HTTPException
from fastapi.responses import ORJSONResponse

from app.settings.globals import ENV


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


class BadResponseError(Exception):
    pass


class InvalidResponseError(Exception):
    pass


class UnauthorizedError(Exception):
    pass


def http_error_handler(exc: HTTPException) -> ORJSONResponse:

    message = exc.detail
    if exc.status_code < 500:
        status = "failed"
    else:
        status = "error"
        # In dev and test print full traceback of internal server errors
        if ENV == "test" or ENV == "dev":
            exc_type, exc_value, exc_traceback = sys.exc_info()
            message = traceback.format_exception(exc_type, exc_value, exc_traceback)
    return ORJSONResponse(
        status_code=exc.status_code, content={"status": status, "message": message}
    )
