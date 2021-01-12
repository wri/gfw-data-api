from fastapi import Response


class CSVResponse(Response):
    media_type = "text/csv"
