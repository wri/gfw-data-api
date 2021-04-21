from app.application import db
from app.models.orm.base import Base


class ApiKey(Base):
    __tablename__ = "api_keys"
    api_key = db.Column(db.UUID, primary_key=True)
    organization = db.Column(db.String, nullable=False)
    email = db.Column(db.String, nullable=False)
    domains = db.Column(db.ARRAY(db.String), nullable=False)
    expiration_date = db.Column(db.DateTime)
