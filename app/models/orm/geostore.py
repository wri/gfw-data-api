from .base import Base, db

########
# NOTE #
########
# UserAreas doesn't officially inherit from Geostore in a class hierarchy, but it
# DOES inherit in the DB (via a custom migration). So any time you change the
# Geostore table, change UserAreas as well! And vice versa, of course.


class Geostore(Base):
    __tablename__ = "geostore"

    gfw_geostore_id = db.Column(db.UUID, primary_key=True)
    gfw_geojson = db.Column(db.TEXT)
    gfw_area__ha = db.Column(db.Numeric)
    gfw_bbox = db.Column(db.ARRAY(db.Numeric))

    _geostore_gfw_geostore_id_idx = db.Index(
        "geostore_gfw_geostore_id_idx", "gfw_geostore_id", postgresql_using="hash"
    )
