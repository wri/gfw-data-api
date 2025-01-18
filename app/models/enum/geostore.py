from enum import Enum


class GeostoreOrigin(str, Enum):
    gfw = "gfw"
    rw = "rw"


class LandUseType(str, Enum):
    fiber = "fiber"
    logging = "logging"
    # mining = "mining"  # Present in the docs, but yields a 404
    oilpalm = "oilpalm"
    tiger_conservation_landscapes = "tiger_conservation_landscapes"


class LandUseTypeUseString(str, Enum):
    fiber = "gfw_wood_fiber"
    logging = "gfw_logging"
    oilpalm = "gfw_oil_palm"
    tiger_conservation_landscapes = "tcl"
