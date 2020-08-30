from enum import Enum

# These enums are based on the valid options for the various inputs
# to pixETL, and should match whatever is specified in that repo.
# We ALSO define them here in order to catch bad input as soon as possible
# despite the code duplication.


class DataType(str, Enum):
    boolean = "boolean"
    uint = "uint"
    int = "int"
    uint16 = "uint16"
    int16 = "int16"
    uint32 = "uint32"
    int32 = "int32"
    float16 = "float16"
    half = "half"
    float32 = "float32"
    single = "single"
    float64 = "float64"
    double = "double"


class Grid(str, Enum):
    one_by_four_thousand = "1/4000"
    three_by_thirty_three_thousand_six_hundred = "3/33600"
    three_by_fifty_thousand = "3/50000"
    eight_by_thirty_two_thousand = "8/32000"
    ten_by_forty_thousand = "10/40000"
    ninety_by_nine_thousand_nine_hundred_eighty_four = "90/9984"
    ninety_by_twenty_seven_thousand_eight = "90/27008"


class Order(str, Enum):
    asc = "asc"
    desc = "desc"


class RasterizeMethod(str, Enum):
    _count = "count"
    value = "value"


class ResamplingMethod(str, Enum):
    average = "average"
    bilinear = "bilinear"
    cubic = "cubic"
    cubic_spline = "cubic_spline"
    lanczos = "lanczos"
    max = "max"
    min = "min"
    med = "med"
    mode = "mode"
    nearest = "nearest"
    q1 = "q1"
    q3 = "q3"
