#!/usr/bin/env python

import csv
from typing import Dict, List, Optional, Type, Union

import click
import pandas
from shapely import wkb
from shapely.geometry import (
    GeometryCollection,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
)
from shapely.geometry.base import BaseGeometry

MultiGeometry = Union[MultiPolygon, MultiLineString, MultiPoint]


@click.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", required=False)
@click.option("--delimiter", default="\t", help="Delimiter")
def cli(input_file: str, output_file: Optional[str], delimiter: str) -> None:

    if not output_file:
        output_file = input_file

    df = pandas.read_csv(input_file, delimiter=delimiter, header=0)
    df["geom"] = df["geom"].map(lambda x: extract(wkb.loads(x, hex=True)))

    df.to_csv(
        output_file,
        sep=delimiter,
        header=True,
        index=False,
        quoting=csv.QUOTE_MINIMAL,
        quotechar='"',
    )


def extract(
    geometry: Union[BaseGeometry, GeometryCollection], geom_type: str = "Polygon"
) -> BaseGeometry:
    new_geometry_type: Dict[str, Type[MultiGeometry]] = {
        "Polygon": MultiPolygon,
        "LineString": MultiLineString,
        "Point": MultiPoint,
    }

    if geometry.geometryType() == "GeometryCollection":
        geom_buffer: List[BaseGeometry] = list()
        for geom in geometry.geoms:
            if geom.geometryType() == geom_type:
                geom_buffer.append(geom)
            elif geom.geometryType() == f"Multi{geom_type}":
                for g in geom.geoms:
                    geom_buffer.append(g)
        new_geom: MultiGeometry = new_geometry_type[geom_type](geom_buffer)
        return new_geom.wkb_hex
    else:
        return geometry.wkb_hex


if __name__ == "__main__":
    cli()
