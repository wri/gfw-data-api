from fastapi import HTTPException, Request
from fastapi.responses import ORJSONResponse, RedirectResponse

from .application import ContextEngine
from .crud.versions import get_latest_version


async def set_db_mode(request: Request, call_next):
    """

    This middleware replaces the db engine depending on the request type.
    Read requests use the read only pool.
    Write requests use the write pool.

    """
    if request.method in ["PUT", "PATCH", "POST", "DELETE"]:
        method = "WRITE"
    else:
        method = "READ"
    async with ContextEngine(method):
        response = await call_next(request)
    return response


async def redirect_latest(request: Request, call_next):
    """

    Redirect all GET requests using latest version to actual version number.

    """

    try:
        if request.method == "GET" and "latest" in request.url.path:
            path_items = request.url.path.split("/")

            i = 0
            for i, item in enumerate(path_items):
                if item == "latest":
                    break
            if i == 0:
                raise HTTPException(status_code=400, detail="Invalid URI")
            path_items[i] = await get_latest_version(path_items[i - 1])
            url = "/".join(path_items)
            return RedirectResponse(url=f"{url}?{request.query_params}")
        else:
            response = await call_next(request)
            return response
    except HTTPException as e:
        return ORJSONResponse(status_code=e.status_code, content=e.detail)
