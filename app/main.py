import json
import logging
import sys

from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from .application import app
from .errors import ClientError, ServerError
from .middleware import redirect_latest, set_db_mode
from .routes import security
from .routes.assets import asset, assets
from .routes.datasets import asset as version_asset
from .routes.datasets import dataset, datasets, features, versions
from .routes.geostore import geostore
from .routes.sql import queries
from .routes.tasks import tasks

################
# LOGGING
################

gunicorn_logger = logging.getLogger("gunicorn.error")
logger.handlers = gunicorn_logger.handlers
sys.path.extend(["./"])


################
# ERRORS
################


@app.exception_handler(ClientError)
async def client_error_handler(request: Request, exc: ClientError):
    return JSONResponse(
        status_code=exc.status_code, content={"status": "failed", "data": exc.detail}
    )


@app.exception_handler(ServerError)
async def server_error_handler(request: Request, exc: ServerError):
    return JSONResponse(
        status_code=exc.status_code, content={"status": "error", "message": exc.detail}
    )


@app.exception_handler(HTTPException)
async def httpexception_error_handler(request: Request, exc: HTTPException):
    if exc.status_code < 500:
        status = "failed"
    else:
        status = "error"
    return JSONResponse(
        status_code=exc.status_code, content={"status": status, "message": exc.detail}
    )


@app.exception_handler(RequestValidationError)
async def rve_error_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
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
    version_asset.router,
)

for r in dataset_routers:
    app.include_router(r, prefix="/dataset")


###############
# ASSET API
###############

app.include_router(assets.router, prefix="/assets")
app.include_router(asset.router, prefix="/asset")


###############
# FEATURE API
###############


feature_routers = (features.router,)

for r in feature_routers:
    app.include_router(r, prefix="/features")

###############
# SQL API
###############

sql_routers = (queries.router,)

for r in sql_routers:
    app.include_router(r, prefix="/sql")

###############
# GEOSTORE API
###############

geostore_routers = (geostore.router,)

for r in geostore_routers:
    app.include_router(r, prefix="/geostore")


###############
# TASK API
###############

task_routers = (tasks.router,)
for r in task_routers:
    app.include_router(r, prefix="/tasks")

#######################
# OPENAPI Documentation
#######################


tags_metadata = [
    {"name": "Dataset", "description": datasets.__doc__},
    {"name": "Version", "description": versions.__doc__},
    {"name": "Assets", "description": asset.__doc__},
    {"name": "Query", "description": queries.__doc__},
    {"name": "Geostore", "description": geostore.__doc__},
    {"name": "Tasks", "description": tasks.__doc__},
]


def custom_openapi(openapi_prefix: str = ""):
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="GFW DATA API",
        version="0.1.0",
        description="Use GFW DATA API to explore, manage and access data.",
        routes=app.routes,
        openapi_prefix=openapi_prefix,
    )

    openapi_schema["tags"] = tags_metadata
    openapi_schema["info"]["x-logo"] = {"url": "/static/gfw-data-api.png"}
    openapi_schema["x-tagGroups"] = [
        {"name": "Dataset API", "tags": ["Datasets", "Versions", "Assets"]},
        {"name": "Geostore API", "tags": ["Geostore"]},
        {"name": "SQL API", "tags": ["Query"]},
        {"name": "Task API", "tags": ["Tasks"]},
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
