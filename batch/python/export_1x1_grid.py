#!/usr/bin/env python

import argparse
import asyncio
import os
from asyncio import AbstractEventLoop
from typing import List, Set, Tuple

import asyncpg
from asyncpg.exceptions import ConnectionDoesNotExistError
from logger import get_logger
from sqlalchemy import Table, column, literal_column, select, table, text
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import TextClause

PGPASSWORD = os.environ.get("PGPASSWORD", None)
PGHOST = os.environ.get("PGHOST", None)
PGPORT = os.environ.get("PGPORT", None)
PGDATABASE = os.environ.get("PGDATABASE", None)
PGUSER = os.environ.get("PGUSER", None)
MAX_TASKS = int(os.environ.get("MAX_TASKS", 1))

LOGGER = get_logger(__name__)

tiles: List[Tuple[str, bool, bool]] = [
    ("00N_000E", True, True),
    ("00N_010E", True, True),
    ("00N_020E", True, True),
    ("00N_030E", True, True),
    ("00N_040E", True, True),
    ("00N_050E", False, True),
    ("00N_040W", True, True),
    ("00N_050W", True, True),
    ("00N_060W", True, True),
    ("00N_070W", True, True),
    ("00N_080W", True, True),
    ("00N_090E", True, True),
    ("00N_090W", True, True),
    ("00N_100E", True, True),
    ("00N_100W", True, True),
    ("00N_110E", True, True),
    ("00N_120E", True, True),
    ("00N_130E", True, True),
    ("00N_140E", True, True),
    ("00N_150E", True, True),
    ("00N_160E", True, True),
    ("10N_000E", True, True),
    ("10N_010E", True, True),
    ("10N_010W", True, True),
    ("10N_020E", True, True),
    ("10N_020W", True, True),
    ("10N_030E", True, True),
    ("10N_040E", True, True),
    ("10N_050E", True, True),
    ("10N_050W", True, True),
    ("10N_060W", True, True),
    ("10N_070E", True, True),
    ("10N_070W", True, True),
    ("10N_080E", True, True),
    ("10N_080W", True, True),
    ("10N_090E", True, True),
    ("10N_090W", True, True),
    ("10N_100E", True, True),
    ("10N_100W", True, True),
    ("10N_110E", True, True),
    ("10N_120E", True, True),
    ("10N_130E", True, True),
    ("10S_010E", True, True),
    ("10S_020E", True, True),
    ("10S_030E", True, True),
    ("10S_040E", True, True),
    ("10S_040W", True, True),
    ("10S_050E", True, True),
    ("10S_050W", True, True),
    ("10S_060W", True, True),
    ("10S_070W", True, True),
    ("10S_080W", True, True),
    ("10S_110E", True, True),
    ("10S_120E", True, True),
    ("10S_130E", True, True),
    ("10S_140E", True, True),
    ("10S_150E", True, True),
    ("10S_160E", True, True),
    ("10S_170E", True, True),
    ("20N_000E", True, True),
    ("20N_010E", True, True),
    ("20N_010W", True, True),
    ("20N_020E", True, True),
    ("20N_020W", True, True),
    ("20N_030E", True, True),
    ("20N_030W", True, True),
    ("20N_040E", True, True),
    ("20N_050E", True, True),
    ("20N_060W", True, True),
    ("20N_070E", True, True),
    ("20N_070W", True, True),
    ("20N_080E", True, True),
    ("20N_080W", True, True),
    ("20N_090E", True, True),
    ("20N_090W", True, True),
    ("20N_100E", True, True),
    ("20N_100W", True, True),
    ("20N_110E", True, True),
    ("20N_110W", True, True),
    ("20N_120E", True, True),
    ("20N_160W", True, False),
    ("20S_010E", True, True),
    ("20S_020E", True, True),
    ("20S_030E", True, True),
    ("20S_040E", True, True),
    ("20S_050E", True, True),
    ("20S_050W", True, True),
    ("20S_060W", True, True),
    ("20S_070W", True, True),
    ("20S_080W", True, True),
    ("20S_110E", True, True),
    ("20S_120E", True, True),
    ("20S_130E", True, True),
    ("20S_140E", True, True),
    ("20S_150E", True, True),
    ("20S_160E", True, True),
    ("30N_000E", True, True),
    ("30N_010E", True, True),
    ("30N_010W", True, True),
    ("30N_020E", True, True),
    ("30N_020W", True, True),
    ("30N_030E", True, True),
    ("30N_040E", True, True),
    ("30N_050E", True, True),
    ("30N_060E", True, True),
    ("30N_070E", True, True),
    ("30N_080E", True, True),
    ("30N_080W", True, True),
    ("30N_090E", True, True),
    ("30N_090W", True, True),
    ("30N_100E", True, True),
    ("30N_100W", True, True),
    ("30N_110E", True, True),
    ("30N_110W", True, True),
    ("30N_120E", True, True),
    ("30N_120W", True, True),
    ("30N_130E", True, True),
    ("30N_160W", True, False),
    ("30N_170W", True, False),
    ("30S_010E", True, False),
    ("30S_020E", True, False),
    ("30S_030E", True, False),
    ("30S_060W", True, True),
    ("30S_070W", True, False),
    ("30S_080W", True, False),
    ("30S_110E", True, False),
    ("30S_120E", True, False),
    ("30S_130E", True, False),
    ("30S_140E", True, False),
    ("30S_150E", True, False),
    ("30S_170E", True, False),
    ("40N_000E", True, False),
    ("40N_010E", True, False),
    ("40N_010W", True, False),
    ("40N_020E", True, False),
    ("40N_020W", True, False),
    ("40N_030E", True, False),
    ("40N_040E", True, False),
    ("40N_050E", True, False),
    ("40N_060E", True, False),
    ("40N_070E", True, False),
    ("40N_070W", True, False),
    ("40N_080E", True, False),
    ("40N_080W", True, False),
    ("40N_090E", True, False),
    ("40N_090W", True, False),
    ("40N_100E", True, False),
    ("40N_100W", True, False),
    ("40N_110E", True, False),
    ("40N_110W", True, False),
    ("40N_120E", True, False),
    ("40N_120W", True, False),
    ("40N_130E", True, False),
    ("40N_130W", True, False),
    ("40N_140E", True, False),
    ("40S_070W", True, False),
    ("40S_080W", True, False),
    ("40S_140E", True, False),
    ("40S_160E", True, False),
    ("40S_170E", True, False),
    ("50N_000E", True, False),
    ("50N_010E", True, False),
    ("50N_010W", True, False),
    ("50N_020E", True, False),
    ("50N_030E", True, False),
    ("50N_040E", True, False),
    ("50N_050E", True, False),
    ("50N_060E", True, False),
    ("50N_060W", True, False),
    ("50N_070E", True, False),
    ("50N_070W", True, False),
    ("50N_080E", True, False),
    ("50N_080W", True, False),
    ("50N_090E", True, False),
    ("50N_090W", True, False),
    ("50N_100E", True, False),
    ("50N_100W", True, False),
    ("50N_110E", True, False),
    ("50N_110W", True, False),
    ("50N_120E", True, False),
    ("50N_120W", True, False),
    ("50N_130E", True, False),
    ("50N_130W", True, False),
    ("50N_140E", True, False),
    ("50N_150E", True, False),
    ("50S_060W", True, False),
    ("50S_070W", True, False),
    ("50S_080W", True, False),
    ("60N_000E", True, False),
    ("60N_010E", True, False),
    ("60N_010W", True, False),
    ("60N_020E", True, False),
    ("60N_020W", True, False),
    ("60N_030E", True, False),
    ("60N_040E", True, False),
    ("60N_050E", True, False),
    ("60N_060E", True, False),
    ("60N_060W", True, False),
    ("60N_070E", True, False),
    ("60N_070W", True, False),
    ("60N_080E", True, False),
    ("60N_080W", True, False),
    ("60N_090E", True, False),
    ("60N_090W", True, False),
    ("60N_100E", True, False),
    ("60N_100W", True, False),
    ("60N_110E", True, False),
    ("60N_110W", True, False),
    ("60N_120E", True, False),
    ("60N_120W", True, False),
    ("60N_130E", True, False),
    ("60N_130W", True, False),
    ("60N_140E", True, False),
    ("60N_140W", True, False),
    ("60N_150E", True, False),
    ("60N_150W", True, False),
    ("60N_160E", True, False),
    ("60N_160W", True, False),
    ("60N_170E", True, False),
    ("60N_170W", True, False),
    ("60N_180W", True, False),
    ("70N_000E", True, False),
    ("70N_010E", True, False),
    ("70N_010W", True, False),
    ("70N_020E", True, False),
    ("70N_020W", True, False),
    ("70N_030E", True, False),
    ("70N_030W", True, False),
    ("70N_040E", True, False),
    ("70N_050E", True, False),
    ("70N_060E", True, False),
    ("70N_070E", True, False),
    ("70N_070W", True, False),
    ("70N_080E", True, False),
    ("70N_080W", True, False),
    ("70N_090E", True, False),
    ("70N_090W", True, False),
    ("70N_100E", True, False),
    ("70N_100W", True, False),
    ("70N_110E", True, False),
    ("70N_110W", True, False),
    ("70N_120E", True, False),
    ("70N_120W", True, False),
    ("70N_130E", True, False),
    ("70N_130W", True, False),
    ("70N_140E", True, False),
    ("70N_140W", True, False),
    ("70N_150E", True, False),
    ("70N_150W", True, False),
    ("70N_160E", True, False),
    ("70N_160W", True, False),
    ("70N_170E", True, False),
    ("70N_170W", True, False),
    ("70N_180W", True, False),
    ("80N_010E", True, False),
    ("80N_020E", True, False),
    ("80N_030E", True, False),
    ("80N_050E", True, False),
    ("80N_060E", True, False),
    ("80N_070E", True, False),
    ("80N_070W", True, False),
    ("80N_080E", True, False),
    ("80N_080W", True, False),
    ("80N_090E", True, False),
    ("80N_090W", True, False),
    ("80N_100E", True, False),
    ("80N_100W", True, False),
    ("80N_110E", True, False),
    ("80N_110W", True, False),
    ("80N_120E", True, False),
    ("80N_120W", True, False),
    ("80N_130E", True, False),
    ("80N_130W", True, False),
    ("80N_140E", True, False),
    ("80N_140W", True, False),
    ("80N_150E", True, False),
    ("80N_150W", True, False),
    ("80N_160E", True, False),
    ("80N_160W", True, False),
    ("80N_170E", True, False),
    ("80N_170W", True, False),
]

intersection: TextClause = text(
    """ST_SnapToGrid(
        ST_Buffer(ST_Buffer(
            ST_MakeValid(
                ST_SimplifyPreserveTopology(
                    ST_Intersection(
                        t.geom,
                        g.geom),
                    0.0001)
                ),
            0.0001),-0.0001),0.000000001)"""
)


intersect_filter: TextClause = text(
    """ST_Intersects(
        t.geom,
        g.geom)"""
)


def extract_polygon(geometry: TextClause) -> TextClause:
    """Extracting geometries from collection in postgis is buggy.

    There were often still some remaining Geometry colletions persents.
    Keeping this function around, but will use a postprocessing step
    using pandas and shapely instead.
    """
    return text(
        f"""
            CASE
            WHEN ST_GeometryType({str(geometry)}) = 'ST_GeometryCollection'::text
                THEN ST_CollectionExtract({str(geometry)}, 3)
            ELSE {str(geometry)}
            END
            """
    )


def grid_filter(grid_id: str) -> TextClause:
    return text(f"""gfw_grid_10x10_id = '{grid_id}'""")


def src_table(dataset: str, version: str) -> Table:
    src_table: Table = table(version)
    src_table.schema = dataset
    return src_table


def get_sql(
    dataset: str, version: str, fields: List[str], grid_id: str, tcl: bool, glad: bool
) -> Select:
    """Generate SQL statement."""

    geom_column = literal_column(str(intersection)).label("geom")
    tcl_column = literal_column(str(tcl)).label("tcl")
    glad_column = literal_column(str(glad)).label("glad")
    nested_columns = [field.split(",") for field in fields]
    columns = [column(c) for columns in nested_columns for c in columns]

    sql: Select = (
        select(columns + [tcl_column, glad_column, geom_column])
        .select_from(src_table(dataset, version).alias("t"))
        .select_from(table("gfw_grid_1x1").alias("g"))
        .where(intersect_filter)
        .where(grid_filter(grid_id))
    )
    LOGGER.info(sql)

    return sql


async def run(
    loop: AbstractEventLoop, dataset: str, version: str, fields: List[str]
) -> None:
    async def copy_tiles(i: int, tile: Tuple[str, bool, bool]) -> None:
        if i == 0:
            output = f"{dataset}_{version}_1x1.tsv"
            header = True
        else:
            output = f"{dataset}_{version}_1x1_part_{i}.tmp"
            header = False

        grid_id: str = tile[0]
        tcl: bool = tile[1]
        glad: bool = tile[2]

        retries = 0
        success = False

        while not success and retries < 2:
            try:
                con = await asyncpg.connect(
                    user=PGUSER,
                    database=PGDATABASE,
                    host=PGHOST,
                    port=PGPORT,
                    password=PGPASSWORD,
                )
                result = await con.copy_from_query(
                    str(get_sql(dataset, version, fields, grid_id, tcl, glad)),
                    output=output,
                    format="csv",
                    delimiter="\t",
                    header=header,
                )
                LOGGER.info(result)

                await con.close()
                success = True
            except ConnectionDoesNotExistError:
                LOGGER.warning("Connection to DB lost during operation, retrying...")
                retries += 1

        if retries >= 2:
            raise Exception(
                "error: failed with ConnectionDoesNotExistError after multiple retries"
            )

    max_tasks: int = MAX_TASKS
    tasks: Set = set()

    for i, tile in enumerate(tiles):
        if len(tasks) >= max_tasks:
            # Wait for some download to finish before adding a new one
            _done, tasks = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
        tasks.add(loop.create_task(copy_tiles(i, tile)))
    await asyncio.wait(tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", "-d", type=str, help="Dataset name")
    parser.add_argument("--version", "-v", type=str, help="Version name")
    parser.add_argument(
        "--column_names", "-C", type=str, nargs="+", help="Column names to include"
    )
    args = parser.parse_args()
    loop: AbstractEventLoop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop, args.dataset, args.version, args.column_names))
