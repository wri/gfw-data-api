from enum import Enum


class ColorMapType(str, Enum):
    discrete = "discrete"
    gradient = "gradient"
    date_conf_intensity = "date_conf_intensity"
