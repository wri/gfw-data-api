import decimal
import io
from typing import Any

import orjson
from fastapi.responses import Response, StreamingResponse
from starlette.background import BackgroundTask


class ORJSONLiteResponse(Response):
    media_type = "application/json"

    def __init__(
        self,
        content: Any = None,
        status_code: int = 200,
        headers: dict = None,
        background: BackgroundTask = None,
    ) -> None:
        serialized_content = orjson.dumps(content, default=jsonencoder_lite)
        super().__init__(
            serialized_content, status_code, headers, self.media_type, background
        )


class ORJSONStreamingResponse(StreamingResponse):
    media_type = "application/json"

    def __init__(
        self,
        content: Any,
        status_code: int = 200,
        headers: dict = None,
        background: BackgroundTask = None,
        filename: str = "export.json",
        download: bool = True,
    ) -> None:
        serialized_content = io.BytesIO(orjson.dumps(content, default=jsonencoder_lite))
        if not headers:
            headers = dict()
        if download:
            headers["Content-Disposition"] = f"attachment; filename={filename}"

        super().__init__(
            serialized_content, status_code, headers, self.media_type, background
        )


class CSVStreamingResponse(StreamingResponse):
    media_type = "text/csv"

    def __init__(
        self,
        content: Any,
        status_code: int = 200,
        headers: dict = None,
        background: BackgroundTask = None,
        filename: str = "export.csv",
        download: bool = True,
    ) -> None:
        if not headers:
            headers = dict()
        if download:
            headers["Content-Disposition"] = f"attachment; filename={filename}"

        super().__init__(content, status_code, headers, self.media_type, background)


def jsonencoder_lite(obj):
    """Custom, lightweight version of FastAPI jsonencoder for serialization of
    large, simple objects.

    jsonencoder is very thorough, but consequently fairly slow for
    encoding large lists. This encoder only encodes the bare necessities
    needed to work with serializers like ORJSON.
    """
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError(
        f"Unknown type for value {obj} with class type {type(obj).__name__}"
    )
