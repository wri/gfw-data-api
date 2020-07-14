from pydantic.main import BaseModel

from app.models.pydantic.responses import Response


class Stats(BaseModel):
    pass


class StatsResponse(Response):
    data: Stats
