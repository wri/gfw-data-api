from typing import Any

from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask


class CSVStreamingResponse(StreamingResponse):
    media_type = "text/csv"

    def __init__(
        self,
        content: Any,
        status_code: int = 200,
        headers: dict = None,
        background: BackgroundTask = None,
        filename: str = "export.csv",
    ) -> None:
        if not headers:
            headers = dict()
        headers["Content-Disposition"] = f"attachment; filename={filename}"

        super().__init__(content, status_code, headers, self.media_type, background)
