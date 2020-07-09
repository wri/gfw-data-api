from typing import List, Optional

from pydantic import BaseModel


class Source(BaseModel):
    source_uri: Optional[List[str]]
