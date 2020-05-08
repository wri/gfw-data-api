import os

import psycopg2
import pendulum
from pendulum.parsing.exceptions import ParserError


def get_sql(sql_tmpl, **kwargs):
    with open(sql_tmpl, "r") as tmpl:
        sql = tmpl.read().format(**kwargs)
    print(sql)
    return sql


def cli():
    connection = psycopg2.connect(
        database=os.environ["POSTGRES_NAME"],
        user=os.environ["POSTGRES_USERNAME"],
        password=os.environ["POSTGRES_PASSWORD"],
        port=os.environ["POSTGRES_PORT"],
        host=os.environ["POSTGRES_HOST"],
    )

    years = range(2011, 2022)
    schema = "nasa_viirs_fire_alerts"
    table = "v202003"

    cursor = connection.cursor()
    print(f"Create table")

    cursor.execute(get_sql("sql/create_table.sql.tmpl", schema=schema, table=table))

    for year in years:
        for week in range(1, 54):
            try:
                week = f"{week:02}"
                start = pendulum.parse(f"{year}-W{week}").to_date_string()
                end = pendulum.parse(f"{year}-W{week}").add(days=7).to_date_string()
                print(f"Create partition for week {week}")
                cursor.execute(
                    get_sql(
                        "sql/create_partitions.sql.tmpl",
                        schema=schema,
                        table=table,
                        year=year,
                        week=week,
                        start=start,
                        end=end,
                    )
                )
            except ParserError:
                # Year has only 52 weeks
                pass

    connection.commit()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    cli()
