import os

import click
import psycopg2


@click.command()
@click.argument("input")
@click.option("--schema", default="nasa_viirs_fire_alerts")
@click.option("--table", default="v202003")
def cli(input, schema, table):
    load(input, schema, table)


def load(input, schema="nasa_viirs_fire_alerts", table="v202003"):

    connection = psycopg2.connect(
        database=os.environ["POSTGRES_NAME"],
        user=os.environ["POSTGRES_USERNAME"],
        password=os.environ["POSTGRES_PASSWORD"],
        port=os.environ["POSTGRES_PORT"],
        host=os.environ["POSTGRES_HOST"],
    )

    cursor = connection.cursor()
    with open(input, "r") as f:
        # Notice that we don't need the `csv` module.
        next(f)  # Skip the header row.
        cursor.copy_from(
            f,
            f"{schema}.{table}",
            columns=(
                "iso",
                "adm1",
                "adm2",
                "longitude",
                "latitude",
                "alert__date",
                "alert__time_utc",
                "confidence__cat",
                "bright_ti4__K",
                "bright_ti5__K",
                "frp__MW",
                "wdpa_protected_area__iucn_cat",
                "is__regional_primary_forest",
                "is__alliance_for_zero_extinction_site",
                "is__key_biodiversity_area",
                "is__landmark",
                "gfw_plantation__type",
                "is__gfw_mining",
                "is__gfw_logging",
                "rspo_oil_palm__certification_status",
                "is__gfw_wood_fiber",
                "is__peat_land",
                "is__idn_forest_moratorium",
                "is__gfw_oil_palm",
                "idn_forest_area__type",
                "per_forest_concession__type",
                "is__gfw_oil_gas",
                "is__mangroves_2016",
                "is__intact_forest_landscapes_2016",
                "bra_biome__name",
                "alert__count",
            ),
        )

    connection.commit()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    cli()
