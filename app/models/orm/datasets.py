from .base import Base, db


class Dataset(Base):
    __tablename__ = "datasets"
    dataset = db.Column(db.String, primary_key=True)
    is_downloadable = db.Column(db.Boolean, nullable=False, default=True)
    owner_id = db.Column(db.String, nullable=True, default=None)
    # metadata = db.Column(db.JSONB, default=dict())
