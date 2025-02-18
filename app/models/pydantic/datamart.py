from uuid import UUID

from .base import StrictBaseModel


class TreeCoverLossByDriverIn(StrictBaseModel):
    geostore_id: UUID
    canopy_cover: int
