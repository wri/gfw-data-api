import decimal


def jsonencoder_lite(obj):
    """
    Custom, lightweight version of FastAPI jsonencoder for serialization of large, simple objects.
    jsonencoder is very thorough, but consequently fairly slow for encoding large lists.
    This encoder only encodes the bare necessities needed to work with serializers like ORJSON.
    """
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError(f"Unknown type for value {obj} with class type {type(obj).__name__}")