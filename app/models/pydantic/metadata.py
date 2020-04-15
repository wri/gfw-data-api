from typing import List, Optional

from pydantic import BaseModel
from datetime import datetime


class Metadata(BaseModel):
    title: Optional[str]
    subtitle: Optional[str]
    function: Optional[str]
    resolution: Optional[str]
    geographic_coverage: Optional[str]
    source: Optional[str]
    update_frequency: Optional[str]
    content_date: Optional[str]
    cautions: Optional[str]
    license: Optional[str]
    overview: Optional[str]
    citation: Optional[str]
    tags: Optional[List[str]]
    last_update: Optional[datetime]
    data_language: Optional[str]
    key_restrictions: Optional[str]
    download: Optional[str]
    analysis: Optional[str]
    scale: Optional[str]
    data_updates: Optional[str]
    added_date: Optional[str]
    why_added: Optional[str]
    other: Optional[str]
    learn_more: Optional[str]




