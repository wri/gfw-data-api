OPENAPI_EXTRA = {
    "parameters": [
        {
            "name": "aoi",
            "in": "query",
            "required": True,
            "style": "deepObject",
            "explode": True,
            "examples": {
                "Geostore Area Of Interest": {
                    "summary": "Geostore Area Of Interest",
                    "description": "Custom area",
                    "value": {
                        "type": "geostore",
                        "geostore_id": "637d378f-93a9-4364-bfa8-95b6afd28c3a",
                    }
                },
                "Admin Area Of Interest": {
                    "summary": "Admin Area Of Interest",
                    "description": "Administrative Boundary",
                    "value": {
                        "type": "admin",
                        "country": "BRA",
                        "region": "12",
                        "subregion": "2",
                    }
                }
            },
            "description": "The Area of Interest",
            "schema": {
                "oneOf": [
                    {"$ref": "#/components/schemas/GeostoreAreaOfInterest"},
                    {"$ref": "#/components/schemas/AdminAreaOfInterest"},
                ]
            }
        },
        {
            "name": "dataset_version",
            "in": "query",
            "required": False,
            "style": "deepObject",
            "explode": True,
            "schema": {
                "type": "object",
                "additionalProperties": {"type": "string"},
            },
            "example": {
                "umd_tree_cover_loss": "v1.11",
                "tsc_tree_cover_loss_drivers": "v2023",
            },
            "description": (
                "Pass dataset version overrides as bracketed query parameters.",
            )
        }
    ]
}