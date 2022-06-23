from typing import Dict

from app.models.pydantic.geostore import Geometry, GeostoreCommon

response_body: Dict = {
    "data": {
        "type": "geoStore",
        "id": "d8907d30eb5ec7e33a68aa31aaf918a4",
        "attributes": {
            "geojson": {
                "crs": {},
                "type": "FeatureCollection",
                "features": [
                    {
                        "geometry": {
                            "coordinates": [
                                [
                                    [13.286161423, 2.22263581],
                                    [13.895623684, 2.613460107],
                                    [14.475367069, 2.43969337],
                                    [15.288956165, 1.338479182],
                                    [13.44381094, 0.682623753],
                                    [13.286161423, 2.22263581],
                                ]
                            ],
                            "type": "Polygon",
                        },
                        "type": "Feature",
                    }
                ],
            },
            "hash": "d8907d30eb5ec7e33a68aa31aaf918a4",  # pragma: allowlist secret
            "provider": {},
            "areaHa": 2950164.393265342,
            "bbox": [13.286161423, 0.682623753, 15.288956165, 2.613460107],
            "lock": False,
            "info": {"use": {}},
        },
    }
}

data: Dict = response_body["data"]["attributes"]
geojson: Dict = data["geojson"]["features"][0]["geometry"]
geometry: Geometry = Geometry.parse_obj(geojson)
geostore_common: GeostoreCommon = GeostoreCommon(
    geostore_id=data["hash"],
    geojson=geometry,
    area__ha=data["areaHa"],
    bbox=data["bbox"],
)
