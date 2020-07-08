from typing import Any, Dict, List

from .responses import Response


class FeatureResponse(Response):
    data: Dict[str, Any]


class FeaturesResponse(Response):
    data: List[Dict[str, Any]]
