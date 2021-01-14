"""Download dataset in different versions."""
import csv
from contextlib import contextmanager
from io import StringIO
from typing import Iterator, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.engine import RowProxy

from ...models.enum.geostore import GeostoreOrigin
from ...responses import CSVResponse
from .. import dataset_dependency, version_dependency
from .queries import _query_dataset

router = APIRouter()


@router.get(
    "/{dataset}/{version}/download/csv",
    response_class=CSVResponse,
    tags=["Query"],
)
async def download_dataset(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    sql: str = Query(..., description="SQL query."),
    geostore_id: Optional[UUID] = Query(None, description="Geostore ID."),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, description="Origin service of geostore ID."
    ),
    filename: str = Query("export.csv", description="Name of export file."),
    delimiter: str = Query(",", description="Delimiter to use for CSV file."),
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented).

    Return results as downloadable CSV file. This endpoint only works
    for datasets with (geo-)database tables.
    """

    data: List[RowProxy] = await _query_dataset(
        dataset, version, sql, geostore_id, geostore_origin
    )

    with orm_to_csv(data, delimiter) as stream:
        response = CSVResponse(iter([stream.getvalue()]), filename=filename)
        return response


@contextmanager
def orm_to_csv(data: List[RowProxy], delimiter=",") -> Iterator[StringIO]:

    """Create a new csv file that represents generated data."""

    csv_file = StringIO()
    try:
        wr = csv.writer(csv_file, quoting=csv.QUOTE_NONNUMERIC, delimiter=delimiter)
        field_names = data[0].keys()
        wr.writerow(field_names)
        for row in data:
            wr.writerow(row.values())
        csv_file.seek(0)
        yield csv_file
    finally:
        csv_file.close()
