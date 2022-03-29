from enum import Enum

# These enums are based on the valid options for the various inputs
# to pixETL, and should match whatever is specified in that repo.
# We ALSO define them here in order to catch bad input as soon as possible
# despite the code duplication.


class DataType(str, Enum):
    boolean = "boolean"
    uint8 = "uint8"
    int8 = "int8"
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
    ten_by_one_hundred_thousand = "10/100000"
    ninety_by_one_thousand_eight = "90/1008"
    ninety_by_nine_thousand_nine_hundred_eighty_four = "90/9984"
    ninety_by_twenty_seven_thousand_eight = "90/27008"
    zoom_0 = "zoom_0"
    zoom_1 = "zoom_1"
    zoom_2 = "zoom_2"
    zoom_3 = "zoom_3"
    zoom_4 = "zoom_4"
    zoom_5 = "zoom_5"
    zoom_6 = "zoom_6"
    zoom_7 = "zoom_7"
    zoom_8 = "zoom_8"
    zoom_9 = "zoom_9"
    zoom_10 = "zoom_10"
    zoom_11 = "zoom_11"
    zoom_12 = "zoom_12"
    zoom_13 = "zoom_13"
    zoom_14 = "zoom_14"
    zoom_15 = "zoom_15"
    zoom_16 = "zoom_16"
    zoom_17 = "zoom_17"
    zoom_18 = "zoom_18"
    zoom_19 = "zoom_19"
    zoom_20 = "zoom_20"
    zoom_21 = "zoom_21"
    zoom_22 = "zoom_22"


class NonNumericFloat(str, Enum):
    # inf = "Inf"
    # neg_inf = "-Inf"
    nan = "nan"


class Order(str, Enum):
    asc = "asc"
    desc = "desc"


class RasterizeMethod(str, Enum):
    count_ = "count"
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


class PhotometricType(str, Enum):
    minisblack = "MINISBLACK"
    miniswhite = "MINISWHITE"
    rgb = "RGB"
    cmyk = "CMYK"
    ycbcr = "YCBCR"
    cielab = "CIELAB"
    icclab = "ICCLAB"
    itulab = "ITULAB"
