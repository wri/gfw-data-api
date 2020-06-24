import logging
import sys

from fastapi.logger import logger
from fastapi.openapi.utils import get_openapi
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from .application import app
from .middleware import redirect_latest, set_db_mode
from .routes import security
from .routes.features import features
from .routes.geostore import geostore
from .routes.meta import assets, datasets, tasks, versions
from .routes.sql import queries

###############
# LOGGING
################

gunicorn_logger = logging.getLogger("gunicorn.error")
logger.handlers = gunicorn_logger.handlers
sys.path.extend(["./"])

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

###############
# AUTHENTICATION
################

app.include_router(security.router, tags=["Authentication"])

###############
# META API
###############

meta_routers = (
    tasks.router,
    datasets.router,
    versions.router,
    assets.router,
)

for r in meta_routers:
    app.include_router(r, prefix="/meta")

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

##################
# OPENAPI Documentation
##################


tags_metadata = [
    {"name": "Dataset", "description": datasets.__doc__},
    {"name": "Version", "description": versions.__doc__},
    {"name": "Assets", "description": assets.__doc__},
    {"name": "Tasks", "description": tasks.__doc__},
    {"name": "Features", "description": features.__doc__},
    {"name": "Query", "description": queries.__doc__},
    {"name": "Geostore", "description": geostore.__doc__},
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
        {"name": "Meta API", "tags": ["Datasets", "Versions", "Assets", "Tasks"]},
        {"name": "Geostore API", "tags": ["Geostore"]},
        {"name": "Feature API", "tags": ["Features"]},
        {"name": "SQL API", "tags": ["Query"]},
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
