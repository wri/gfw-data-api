#!/usr/bin/env python

import json
import os

import click
import psycopg2


@click.command()
@click.option("-d", "--dataset", type=str, help="Dataset name")
@click.option("-v", "--version", type=str, help="Version name")
@click.option("-p", "--partition_type", type=str, help="Partition type")
@click.option("-P", "--partition_schema", type=str, help="Partition schema")
def cli(dataset: str, version: str, partition_type: str, partition_schema: str) -> None:

    click.echo(
        f"python create_partition.py -d {dataset} -v {version} -p {partition_type} -P {partition_schema}"
    )

    connection = psycopg2.connect(
        database=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        port=os.environ["PGPORT"],
        host=os.environ["PGHOST"],
    )
    cursor = connection.cursor()

    # HashSchema = int
    if partition_type == "hash":
        partition_count: int = json.loads(partition_schema)["partition_count"]
        for i in range(partition_count):
            sql = f"""
                   CREATE TABLE "{dataset}"."{version}_{i}"
                    PARTITION OF "{dataset}"."{version}"
                    FOR VALUES
                        WITH (MODULUS {partition_count}, REMAINDER {i})
                    """
            click.echo(sql)
            cursor.execute(sql)

    # ListSchema = Dict[str, List[str]]
    elif partition_type == "list":
        partition_list: list = json.loads(partition_schema)
        for partition in partition_list:
            sql = f"""
                    CREATE TABLE "{dataset}"."{version}_{partition["partition_suffix"]}"
                        PARTITION OF "{dataset}"."{version}"
                        FOR VALUES
                            IN {tuple(partition["value_list"])}
                    """
            click.echo(sql)
            cursor.execute(sql)

    # RangeSchema = Dict[str, Tuple[Any, Any]]
    elif partition_type == "range":
        partition_list = json.loads(partition_schema)
        for partition in partition_list:
            sql = f"""CREATE TABLE "{dataset}"."{version}_{partition["partition_suffix"]}"
                        PARTITION OF "{dataset}"."{version}"
                        FOR VALUES
                            FROM ('{partition["start_value"]}')
                            TO ('{partition["end_value"]}')
                    """
            click.echo(sql)
            cursor.execute(sql)
    else:
        NotImplementedError(
            "The Partition type and schema combination is not supported"
        )

    connection.commit()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    cli()
