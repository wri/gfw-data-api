from app.application import db
from app.models.orm.base import Base


class ApiKey(Base):
    __tablename__ = "api_keys"
    alias = db.Column(db.String, nullable=False)
    user_id = db.Column(db.String, nullable=False)
    api_key = db.Column(db.UUID, primary_key=True)
    organization = db.Column(db.String, nullable=False)
    email = db.Column(db.String, nullable=False)
    domains = db.Column(db.ARRAY(db.String), nullable=False)
    expires_on = db.Column(db.DateTime)

    _api_keys_alias_user_id_unique = db.UniqueConstraint(
        "alias", "user_id", name="alias_user_id_uc"
    )
    _api_keys_api_key_idx = db.Index(
        "api_keys_api_key_idx", "api_key", postgresql_using="hash"
    )
    _api_keys_user_id_idx = db.Index(
        "api_keys_user_id_idx", "user_id", postgresql_using="btree"
    )
