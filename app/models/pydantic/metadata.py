from datetime import date, datetime
from typing import Any, List, Optional, Union
from uuid import UUID

from pydantic import Field, validator, BaseModel
from pydantic.utils import GetterDict

from .base import BaseRecord, StrictBaseModel
from .responses import Response


class CommonMetadata(BaseModel):
    resolution: Optional[Union[int, float]]
    geographic_coverage: Optional[str]
    update_frequency: Optional[str]
    scale: Optional[str]
    citation: Optional[str]

    class Config:
        schema_extra = {
            "examples": [
                {
                    "resolution": 10,
                    "geographic_coverage": "Amazon Basin",
                    "update_frequency": "Updated daily, image revisit time every 5 days",
                    "scale": "regional",
                }
            ]
        }


class DatasetMetadata(CommonMetadata):
    title: Optional[str]
    source: Optional[str]
    license: Optional[str]
    data_language: Optional[str]
    overview: Optional[str]

    function: Optional[str]
    cautions: Optional[str]
    key_restrictions: Optional[str]
    tags: Optional[List[str]]
    why_added: Optional[str]
    learn_more: Optional[str]

    class Config:
        schema_extra = {
            "examples": [
                {
                    "title": "Deforestation alerts (GLAD-S2)",
                    "source": "Global Land Analysis and Discovery (GLAD), University of Maryland",
                    "license": "[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)",
                    "data_language": "en",
                    "overview": "This data set is a forest loss alert product developed by the Global Land Analysis and Discovery lab at the University of Maryland. GLAD-S2 alerts utilize data from the European Space Agency's Sentinel-2 mission, which provides optical imagery at a 10m spatial resolution with a 5-day revisit time. The shorter revisit time, when compared to GLAD Landsat alerts, reduces the time to detect forest loss and between the initial detection of forest loss and classification as high confidence. This is particularly advantageous in wet and tropical regions, where persistent cloud cover may delay detections for weeks to months. GLAD-S2 alerts are available for primary forests in the Amazon basin from January 1st 2019 to present, updated daily. New Sentinel-2 images are analyzed as soon as they are acquired. Cloud, shadow, and water are filtered out of each new image, and a forest loss algorithm is applied to all remaining clear land observations. The algorithm relies on the spectral data in each new image in combination with spectral metrics from a baseline period of the previous two years. Alerts become high confidence when at least two of four subsequent observations are flagged as forest loss (this corresponds to 'high', 'medium', and 'low' confidence loss on the GLAD app linked below). The alert date represents the date of forest loss detection. Users can choose to display only high confidence alerts on the map, but keep in mind this will filter out the most recent detections of forest loss. Additionally, forest loss will not be detected again on pixels with high confidence alerts. Alerts that have not become high confidence within 180 days are removed from the data set.",
                    "function": "Identifies areas of primary forest loss  in near real time using Sentinel-2 imagery",
                    "citation": "Pickens, A.H., Hansen, M.C., Adusei, B., and Potapov P. 2020. Sentinel-2 Forest Loss Alert. Global Land Analysis and Discovery (GLAD), University of Maryland.",
                    "cautions": "Results are masked to only within the primary forest mask of [Turubanova et al (2018)](https://iopscience.iop.org/article/10.1088/1748-9326/aacd1c) in the Amazon river basin, with 2001-2018 forest loss from [Hansen et al. (2013)](https://science.sciencemag.org/content/342/6160/850) removed. Alerts that have been detected in two out of four consecutive images are classified as high confidence. Pixels with high confidence alerts cannot be alerted again. The accuracy of this product has not been assessed",
                    "tags": ["Forest Change"],
                    "learn_more": "https://glad.earthengine.app/view/s2-forest-alerts",
                }
            ]
        }


class DatasetMetadataOut(DatasetMetadata, BaseRecord):
    id: UUID


class DatasetMetadataIn(DatasetMetadata):
    pass


class DatasetMetadataUpdate(DatasetMetadataIn):
    pass


class ContentDateRange(StrictBaseModel):
    start_date: Optional[date] = Field(
        None,
        description="Beginning date covered by data",
    )
    end_date: Optional[date] = Field(
        None,
        description="End date covered by data",
    )

    @validator("start_date", "end_date", pre=True)
    def parse_date_str(cls, value):
        return _date_validator(value)


class VersionMetadataGetter(GetterDict):
    def get(self, key: str, default: Any = None) -> Any:
        if key == "content_date_range":
            return {
                "start_date": self._obj.content_start_date,
                "end_date": self._obj.content_end_date,
            }
        else:
            return super(VersionMetadataGetter, self).get(key, default)


class VersionMetadata(CommonMetadata):
    content_date: Optional[date] = Field(
        None,
        description="Date of content.",
    )
    content_date_range: Optional[ContentDateRange] = Field(
        None,
        description="Date range covered by the content",
    )

    last_update: Optional[date] = Field(
        None,
        description="Date the data were last updated",
    )

    @validator("last_update", "content_date", pre=True)
    def parse_date_str(cls, value):
        return _date_validator(value)

    class Config:
        schema_extra = {
            "examples": [
                {
                    "content_date_range": {
                        "start_date": "2000-01-01",  # TODO fix date
                        "end_date": "2021-04-06",
                    },
                }
            ]
        }


class VersionMetadataIn(VersionMetadata):
    pass


class VersionMetadataOut(VersionMetadata, BaseRecord):
    id: UUID

    class Config:
        getter_dict = VersionMetadataGetter


class VersionMetadataOutWithParent(VersionMetadataOut):
    dataset_metadata: DatasetMetadataOut


class VersionMetadataUpdate(VersionMetadataIn):
    content_date: Optional[date] = Field(
        None,
        description="Date of content",
    )
    content_date_range: Optional[ContentDateRange] = Field(
        None,
        description="Date range covered by the content",
    )

    last_update: Optional[date] = Field(
        None,
        description="Date the data were last updated",
    )


class DatasetMetadataResponse(Response):
    data: DatasetMetadataOut


class VersionMetadataResponse(Response):
    data: VersionMetadataOut


class VersionMetadataWithParentResponse(Response):
    data: VersionMetadataOutWithParent


def _date_validator(date_str):
    if isinstance(date_str, date):
        return date_str

    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
