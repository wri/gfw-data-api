import sys

from fastapi.openapi.utils import get_openapi

from .application import app
from .routes import (
    assets,
    datasets,
    features,
    geostore,
    query,
    security,
    sources,
    versions,
)

sys.path.extend(["./"])


ROUTERS = (
    datasets.router,
    versions.router,
    sources.router,
    assets.router,
    query.router,
    features.router,
    geostore.router,
    security.router,
)

for r in ROUTERS:
    app.include_router(r)

tags_desc_list = [
    {
        "name": "Dataset",
        "description": "Datasets are just a bucket, for datasets which share the same core metadata",
    },
    {
        "name": "Version",
        "description": """Datasets can have different versions. Versions aer usually
                  linked to different releases. Versions can be either mutable (data can change) or immutable (data
                  cannot change). By default versions are immutable. Every version needs one or many source files.
                  These files can be a remote, publicly accessible URL or an uploaded file. Based on the source file(s),
                  users can create additional assets and activate additional endpoints to view and query the dataset.
                  Available assets and endpoints to choose from depend on the source type.
                  """,
    },
    {
        "name": "Sources",
        "description": """Sources are input files to seed new dataset version. Supported types are

* Shapefiles
* File Geodatabase
* GeoTIFF
* CSV
* TSV
* GeoJSON
                  """,
    },
    {
        "name": "Assets",
        "description": """Assets are replicas of the original source files. Assets might
                  be served in different formats, attribute values might be altered, additional attributes added,
                  and feature resolution might have changed. Assets are either managed or unmanaged. Managed assets
                  are created by the API and users can rely on data integrity. Unmanaged assets are only loosly linked
                  to a dataset version and users must cannot rely on full integrety. We can only assume that unmanaged
                  are based on the same version and do not know the processing history.""",
    },
    {
        "name": "Features",
        "description": """Explore data entries for a given dataset version
                  (vector and tablular data only) in a classic RESTful way""",
    },
    {
        "name": "Query",
        "description": """Explore data wentries for a given dataset version using standard SQL""",
    },
    {
        "name": "Geostore",
        "description": """Retrieve a geometry using its mb5 hash for a given dataset,
                  user defined geometries in the datastore""",
    },
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

    uvicorn.run(app, host="0.0.0.0", port=8888, log_level="info")
