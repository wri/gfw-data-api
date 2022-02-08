from .base import Base, db
from .mixins import MetadataMixin


class DatasetMetadata(Base, MetadataMixin):
    __tablename__ = "dataset_metadata"

    metadata_id = db.Column(db.UUID, primary_key=True)
    dataset = db.Column(db.String, nullable=False)
    source = db.Column(db.String, nullable=False)
    license = db.Column(db.String)
    data_language = db.Column(db.String, nullable=False)
    overview = db.Column(db.String, nullable=False)

    citation = db.Column(db.String)
    function = db.Column(db.String)
    cautions = db.Column(db.String)
    key_restrictions = db.Column(db.String)
    keywords = db.Column(db.String)
    why_added = db.Column(db.String)
    learn_more = db.Column(db.String)

    fk = db.ForeignKeyConstraint(
        ["dataset"],
        ["datasets.dataset"],
        name="fk",
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
