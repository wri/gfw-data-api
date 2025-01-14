from enum import Enum


class GeostoreOrigin(str, Enum):
    gfw = "gfw"
    rw = "rw"


class LandUseType(str, Enum):
    fiber = "fiber"
    logging = "logging"
    mining = "mining"
    oil_palm = "oil_palm"
    tiger_conservation_landscapes = "tiger_conservation_landscapes"


class LandUseTypeUseString(str, Enum):
    fiber = "fiber"
    logging = "gfw_logging"
    mining = "mining"
    oil_palm = "oil_palm"
    tiger_conservation_landscapes = "tiger_conservation_landscapes"
