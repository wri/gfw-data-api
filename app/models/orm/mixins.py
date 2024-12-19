from .base import db


class MetadataMixin:
    title = db.Column(db.String)
    subtitle = db.Column(db.String)
    spatial_resolution = db.Column(db.Numeric)
    resolution_description = db.Column(db.String)
    geographic_coverage = db.Column(db.String)
    update_frequency = db.Column(db.String)
    citation = db.Column(db.String)
    scale = db.Column(db.String)
