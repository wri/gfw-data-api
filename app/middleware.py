from fastapi import HTTPException, Request
from fastapi.logger import logger
from fastapi.responses import ORJSONResponse, RedirectResponse

from .application import ContextEngine
from .crud.aliases import get_alias
from .crud.versions import get_latest_version
from .errors import BadRequestError, RecordNotFoundError, http_error_handler


async def set_db_mode(request: Request, call_next):
    """This middleware replaces the db engine depending on the request type.

    Read requests use the read only pool. Write requests use the write
    pool.
    """
    if request.method in ["PUT", "PATCH", "POST", "DELETE"]:
        method = "WRITE"
    else:
        method = "READ"
    async with ContextEngine(method):
        response = await call_next(request)
    return response


async def redirect_latest(request: Request, call_next):
    """Redirect all GET requests using latest version to actual version number.

    Redirect only POST requests for query and download endpoints, as
    other POST endpoints will require to list version number explicitly.
    """
    if (request.method == "GET" and "latest" in request.url.path) or (
        request.method == "POST"
        and "latest" in request.url.path
        and ("query" in request.url.path or "download" in request.url.path)
    ):
        try:
            path_items = request.url.path.split("/")

            i = 0
            for i, item in enumerate(path_items):
                if item == "latest":
                    break
            if i == 0:

                raise BadRequestError("Invalid URI")
            path_items[i] = await get_latest_version(path_items[i - 1])
            url = "/".join(path_items)
            if request.query_params:
                url = f"{url}?{request.query_params}"
            return RedirectResponse(url=url)

        except BadRequestError as e:
            return ORJSONResponse(
                status_code=400, content={"status": "failed", "message": str(e)}
            )

        except RecordNotFoundError as e:
            return ORJSONResponse(
                status_code=404, content={"status": "failed", "message": str(e)}
            )

        except HTTPException as e:
            return http_error_handler(e)

        except Exception as e:
            logger.exception(str(e))
            return ORJSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": "Internal Server Error. Could not process request.",
                },
            )
    else:
        response = await call_next(request)
        return response


async def redirect_alias_to_version(request: Request, call_next):
    """Redirect version request by alias to the actual dataset version.

    For GET requests matching /dataset/{dataset}/{version} OR POST
    requests matching /dataset/{dataset}/{version}/download or
    /dataset/{dataset}/{version}/query, this will check if {version}
    matches an existing alias and redirect to the dataset version
    associated with the alias.
    """

    path_items = request.url.path.split("/")
    is_dataset_version_path = len(path_items) >= 4 and path_items[1] == "dataset"
    is_allowed_post_request = request.method == "POST" and (
        "query" in request.url.path or "download" in request.url.path
    )
    if not is_dataset_version_path:
        response = await call_next(request)
        return response
    if request.method not in ["GET", "POST"] or (
        request.method == "POST" and not is_allowed_post_request
    ):
        response = await call_next(request)
        return response

    dataset, version = path_items[2:4]
    try:
        alias = await get_alias(dataset, version)
        path_items[3] = alias.version
        url = "/".join(path_items)
        if request.query_params:
            url = f"{url}?{request.query_params}"
        return RedirectResponse(url=url)

    except RecordNotFoundError:
        response = await call_next(request)
        return response


async def no_cache_response_header(request: Request, call_next):
    """This middleware adds a cache control response header.

    By default, specify no-cache. Individual endpoints can override this
    header.
    """
    no_cache_endpoints = ["/", "/openapi.json", "docs"]
    response = await call_next(request)

    if request.method == "GET" and request.url.path in no_cache_endpoints:
        response.headers["Cache-Control"] = "no-cache"
    elif request.method == "GET" and response.status_code < 300:
        max_age = response.headers.get("Cache-Control", "max-age=0")
        response.headers["Cache-Control"] = max_age

    return response
