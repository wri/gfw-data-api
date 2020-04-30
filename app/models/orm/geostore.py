from .base import Base, db


class Geostore(Base):
    __tablename__ = "geostore"

    gfw_geostore_id = db.Column(db.UUID, primary_key=True)
    gfw_geojson = (db.Column(db.String, nullable=False),)
    gfw_area__ha = db.Column(db.Numeric, nullable=False)
    gfw_bbox = db.Column(
        db.Geometry("Polygon", 4326), nullable=False
    )  # TODO check if this is the correct type

    _geostore_gfw_geostore_id_idx = db.Index(
        "geostore_gfw_geostore_id_idx", "gfw_geostore_id", postgresql_using="hash"
    )
