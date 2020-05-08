import os
from typing import List, Tuple

import concurrent.futures

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pendulum
from pendulum.parsing.exceptions import ParserError

A_POOL = None
YEARS = range(2011, 2022)
WEEKS = range(1, 54)
SCHEMA = "nasa_viirs_fire_alerts"
TABLE = "v202003"


def cli() -> None:
    """
    Post processing of VIRRS fire data
    -> update geographic columns
    -> create indicies
    -> cluster partitions
    Tasks are run asynchronously for each partition
    """

    pool = get_pool()
    weeks = _get_weeks()

    create_indicies()

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(cluster, weeks)

    pool.closeall()


def get_pool() -> ThreadedConnectionPool:
    """
    The database connection pool
    """
    global A_POOL
    if A_POOL is None:
        A_POOL = psycopg2.pool.ThreadedConnectionPool(
            1,
            10,
            database=os.environ["POSTGRES_NAME"],
            user=os.environ["POSTGRES_USERNAME"],
            password=os.environ["POSTGRES_PASSWORD"],
            port=os.environ["POSTGRES_PORT"],
            host=os.environ["POSTGRES_HOST"],
        )
    return A_POOL


def create_indicies() -> None:
    """
    This creates an invalid index.
    It will be validated automatically, once all partitions are indexed and attached.
    """
    pool = get_pool()
    conn = pool.getconn()

    with conn.cursor() as cursor:
        cursor.execute(
            _get_sql("sql/update_geometry.sql.tmpl", schema=SCHEMA, table=TABLE,)
        )

    with conn.cursor() as cursor:
        cursor.execute(
            _get_sql(
                "sql/create_indicies.sql.tmpl",
                schema=SCHEMA,
                table=TABLE,
                column="geom",
                index="gist",
            )
        )

    with conn.cursor() as cursor:
        cursor.execute(
            _get_sql(
                "sql/create_indicies.sql.tmpl",
                schema=SCHEMA,
                table=TABLE,
                column="geom_wm",
                index="gist",
            )
        )

    with conn.cursor() as cursor:
        cursor.execute(
            _get_sql(
                "sql/create_indicies.sql.tmpl",
                schema=SCHEMA,
                table=TABLE,
                column="alert__date",
                index="btree",
            )
        )


def cluster(weeks: Tuple[int, str]) -> None:
    year = weeks[0]
    week = weeks[1]

    pool = get_pool()
    conn = pool.getconn()
    cursor = conn.cursor()

    cursor.execute(
        _get_sql(
            "sql/cluster_partitions.sql.tmpl",
            schema=SCHEMA,
            table=TABLE,
            year=year,
            week=week,
        )
    )

    cursor.close()

    pool.putconn(conn)


def _get_sql(sql_tmpl, **kwargs) -> str:
    with open(sql_tmpl, "r") as tmpl:
        sql = tmpl.read().format(**kwargs)
    print(sql)
    return sql


def _get_weeks() -> List[Tuple[int, str]]:
    weeks: List[Tuple[int, str]] = list()
    for year in YEARS:
        for week in WEEKS:
            try:
                # Check if year has that many weeks
                pendulum.parse(f"{year}-W{week}")

                week_str = f"{week:02}"
                weeks.append((year, week_str))
            except ParserError:
                # Year has only 52 weeks
                pass
    return weeks


if __name__ == "__main__":
    cli()
