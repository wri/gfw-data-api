from .base import db


class MetadataMixin:
    title = db.Column(db.String)
    resolution = db.Column(db.String)
    geographic_coverage = db.Column(db.String)
    update_frequency = db.Column(db.String)
    citation = db.Column(db.String)
    scale = db.Column(db.String)
