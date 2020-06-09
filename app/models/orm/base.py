from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TEXT, UUID
from sqlalchemy_utils import EmailType, generic_repr

from ...application import db

db.JSONB, db.UUID, db.ARRAY, db.EmailType, db.TEXT, db.Geometry = (
    JSONB,
    UUID,
    ARRAY,
    EmailType,
    TEXT,
    Geometry,
)


@generic_repr
class Base(db.Model):  # type: ignore
    __abstract__ = True
    created_on = db.Column(
        db.DateTime, default=datetime.utcnow, server_default=db.func.now()
    )
    updated_on = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        server_default=db.func.now(),
    )
