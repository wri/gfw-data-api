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
        partition_count: int = int(partition_schema)
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
        partition_dict: dict = json.loads(partition_schema)
        for key in partition_dict.keys():
            sql = f"""
                    CREATE TABLE "{dataset}"."{version}_{key}"
                        PARTITION OF "{dataset}"."{version}"
                        FOR VALUES
                            IN {tuple(partition_dict[key])}
                    """
            click.echo(sql)
            cursor.execute(sql)

    # RangeSchema = Dict[str, Tuple[Any, Any]]
    elif partition_type == "range":
        partition_dict = json.loads(partition_schema)
        for key in partition_dict.keys():
            sql = f"""CREATE TABLE "{dataset}"."{version}_{key}"
                        PARTITION OF "{dataset}"."{version}"
                        FOR VALUES
                            FROM ('{partition_dict[key][0]}')
                            TO ('{partition_dict[key][1]}')
                    """
            # click.echo(sql)
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
