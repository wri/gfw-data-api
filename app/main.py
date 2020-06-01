import logging
import sys

from fastapi.logger import logger
from fastapi.openapi.utils import get_openapi

from .application import app
from .routes import (
    analysis,
    assets,
    datasets,
    features,
    geostore,
    queries,
    security,
    sources,
    versions,
)

gunicorn_logger = logging.getLogger("gunicorn.error")
logger.handlers = gunicorn_logger.handlers
sys.path.extend(["./"])


ROUTERS = (
    datasets.router,
    versions.router,
    sources.router,
    assets.router,
    queries.router,
    features.router,
    geostore.router,
    security.router,
    analysis.router,
)

for r in ROUTERS:
    app.include_router(r)

tags_desc_list = [
    {"name": "Dataset", "description": datasets.description},
    {"name": "Version", "description": versions.description},
    {"name": "Sources", "description": sources.description},
    {"name": "Assets", "description": assets.description},
    {"name": "Features", "description": features.description},
    {"name": "Query", "description": queries.description},
    {"name": "Geostore", "description": geostore.description},
]


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="GFW Data API",
        version="0.1.0",
        description="Use GFW Data API to manage and access GFW Data.",
        routes=app.routes,
    )

    openapi_schema["tags"] = tags_desc_list

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


if __name__ == "__main__":
    import uvicorn

    logger.setLevel(logging.DEBUG)
    uvicorn.run(app, host="0.0.0.0", port=8888, log_level="info")
else:
    logger.setLevel(gunicorn_logger.level)
