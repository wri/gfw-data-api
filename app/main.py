import json
import logging
import sys

from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.requests import Request
from fastapi.responses import ORJSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from app.errors import http_error_handler

from .application import app
from .middleware import redirect_latest, set_db_mode
from .routes import security
from .routes.analysis import analysis
from .routes.assets import asset, assets
from .routes.datasets import asset as version_asset
from .routes.datasets import dataset, datasets, features, geostore, queries, versions
from .routes.geostore import geostore as geostore_top
from .routes.tasks import task

################
# LOGGING
################

gunicorn_logger = logging.getLogger("gunicorn.error")
logger.handlers = gunicorn_logger.handlers
sys.path.extend(["./"])


################
# ERRORS
################


@app.exception_handler(HTTPException)
async def httpexception_error_handler(
    request: Request, exc: HTTPException
) -> ORJSONResponse:
    """Use JSEND protocol for HTTP execptions."""
    return http_error_handler(exc)


@app.exception_handler(RequestValidationError)
async def rve_error_handler(
    request: Request, exc: RequestValidationError
) -> ORJSONResponse:
    """Use JSEND protocol for validation errors."""
    return ORJSONResponse(
        status_code=422, content={"status": "failed", "message": json.loads(exc.json())}
    )


#################
# STATIC FILES
#################

app.mount("/static", StaticFiles(directory="/app/app/static"), name="static")

#################
# MIDDLEWARE
#################

MIDDLEWARE = (set_db_mode, redirect_latest)

for m in MIDDLEWARE:
    app.add_middleware(BaseHTTPMiddleware, dispatch=m)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)
################
# AUTHENTICATION
################

app.include_router(security.router, tags=["Authentication"])

###############
# DATASET API
###############

app.include_router(datasets.router, prefix="/datasets")

dataset_routers = (
    dataset.router,
    versions.router,
    features.router,
    geostore.router,
    version_asset.router,
    queries.router,
)

for r in dataset_routers:
    app.include_router(r, prefix="/dataset")


###############
# ASSET API
###############

app.include_router(assets.router, prefix="/assets")
app.include_router(asset.router, prefix="/asset")


# ###############
# # SQL API
# ###############
#
# sql_routers = (queries.router,)
#
# for r in sql_routers:
#     app.include_router(r, prefix="/sql")

###############
# GEOSTORE API
###############

geostore_routers = (geostore_top.router,)

for r in geostore_routers:
    app.include_router(r, prefix="/geostore")


###############
# TASK API
###############

task_routers = (task.router,)
for r in task_routers:
    app.include_router(r, prefix="/task")

analysis_routers = (analysis.router,)
for r in analysis_routers:
    app.include_router(r, prefix="/analysis")

#######################
# OPENAPI Documentation
#######################


tags_metadata = [
    {"name": "Dataset", "description": datasets.__doc__},
    {"name": "Version", "description": versions.__doc__},
    {"name": "Assets", "description": asset.__doc__},
    {"name": "Query", "description": queries.__doc__},
    {"name": "Geostore", "description": geostore.__doc__},
    {"name": "Tasks", "description": task.__doc__},
    {"name": "Analysis", "description": analysis.__doc__},
]


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="GFW DATA API",
        version="0.1.0",
        description="Use GFW DATA API to explore, manage and access data.",
        routes=app.routes,
    )

    openapi_schema["tags"] = tags_metadata
    openapi_schema["info"]["x-logo"] = {"url": "/static/gfw-data-api.png"}
    openapi_schema["x-tagGroups"] = [
        {"name": "Dataset API", "tags": ["Datasets", "Versions", "Assets"]},
        {"name": "Geostore API", "tags": ["Geostore"]},
        {"name": "SQL API", "tags": ["Query"]},
        {"name": "Task API", "tags": ["Tasks"]},
        {"name": "Analysis API", "tags": ["Analysis"]},
    ]

    app.openapi_schema = openapi_schema

    return app.openapi_schema


app.openapi = custom_openapi

if __name__ == "__main__":
    import uvicorn

    logger.setLevel(logging.DEBUG)
    uvicorn.run(app, host="0.0.0.0", port=8888, log_level="info")
else:
    logger.setLevel(gunicorn_logger.level)
