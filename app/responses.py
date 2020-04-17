import json
from typing import Any
from starlette.responses import JSONResponse

try:
    import json_api_doc
except ImportError:  # pragma: nocover
    json_api_doc = None  # type: ignore


class JSONAPIResponse(JSONResponse):
    media_type = "application/vnd.api+json"

    def render(self, content: Any) -> bytes:
        assert json_api_doc is not None, "json_api_doc must be installed to use ORJSONResponse"
        content = json_api_doc.serialize(content)
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
        ).encode("utf-8")
