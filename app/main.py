import logging
import sys

from fastapi import FastAPI
from fastapi.logger import logger
from starlette.middleware.base import BaseHTTPMiddleware

from app import database

from .application import app
from .middleware import redirect_latest, set_db_mode
from .routes import security
from .routes.features import features
from .routes.geostore import geostore
from .routes.meta import assets, datasets, versions
from .routes.sql import queries

###############
### LOGGING
################

gunicorn_logger = logging.getLogger("gunicorn.error")
logger.handlers = gunicorn_logger.handlers
sys.path.extend(["./"])

#################
### MIDDLEWARE
#################

MIDDLEWARE = (set_db_mode, redirect_latest)

for m in MIDDLEWARE:
    app.add_middleware(BaseHTTPMiddleware, dispatch=m)

###############
### AUTHENTICATION
################

app.include_router(security.router)


###############
### META API
###############


meta_api_tags = [
    {"name": "Dataset", "description": datasets.__doc__},
    {"name": "Version", "description": versions.__doc__},
    {"name": "Assets", "description": assets.__doc__},
]


meta_api = FastAPI(
    title="GFW Meta Data API",
    version="0.1.0",
    description="GFW Meta Data API allows you to retrieve information about GFW data, manage content and create new assets.",
    openapi_tags=meta_api_tags,
)


database.db.init_app(meta_api)

meta_routers = (
    datasets.router,
    versions.router,
    assets.router,
)

for m in MIDDLEWARE:
    meta_api.add_middleware(BaseHTTPMiddleware, dispatch=m)

for r in meta_routers:
    meta_api.include_router(r)

app.mount("/meta", meta_api)


###############
### FEATURE API
###############

feature_api_tags = [{"name": "Features", "description": features.__doc__}]


feature_api = FastAPI(
    title="GFW Feature API",
    version="0.1.0",
    description="GFW Feature API allows you to query selected GFW assets using a standard REST approach",
    openapi_tags=feature_api_tags,
)

feature_routers = (features.router,)

for r in feature_routers:
    feature_api.include_router(r)

app.mount("/features", feature_api)


###############
### SQL API
###############

sql_api_tags = [{"name": "Query", "description": queries.__doc__}]


sql_api = FastAPI(
    title="GFW SQL API",
    version="0.1.0",
    description="GFW Query API allows you to query selected GFW assets using a SQL queries",
    openapi_tags=sql_api_tags,
)

sql_routers = (queries.router,)

for r in sql_routers:
    sql_api.include_router(r)

app.mount("/sql", sql_api)


###############
### GEOSTORE API
###############

geostore_api_tags = [{"name": "Geostore", "description": geostore.__doc__}]


geostore_api = FastAPI(
    title="GFW Geostore API",
    version="0.1.0",
    description="GFW Geostore API allows you to query GFW geometries.",
    openapi_tags=geostore_api_tags,
)

geostore_routers = (geostore.router,)

for r in geostore_routers:
    geostore_api.include_router(r)

app.mount("/geostore", geostore_api)


if __name__ == "__main__":
    import uvicorn

    logger.setLevel(logging.DEBUG)
    uvicorn.run(app, host="0.0.0.0", port=8888, log_level="info")
else:
    logger.setLevel(gunicorn_logger.level)
