"""Create API Key Table.

Revision ID: d62a9b15f844
Revises: 73fb3f5e39b8
Create Date: 2021-05-01 01:29:13.157933
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "d62a9b15f844"  # pragma: allowlist secret
down_revision = "73fb3f5e39b8"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "api_keys",
        sa.Column("alias", sa.String(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("api_key", postgresql.UUID(), nullable=False),
        sa.Column("organization", sa.String(), nullable=False),
        sa.Column("email", sa.String(), nullable=False),
        sa.Column("domains", postgresql.ARRAY(sa.String()), nullable=False),
        sa.Column("expires_on", sa.DateTime(), nullable=True),
        sa.Column(
            "created_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column(
            "updated_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.PrimaryKeyConstraint("api_key"),
    )

    op.create_index(
        "api_keys_api_key_idx",
        "api_keys",
        ["api_key"],
        unique=False,
        postgresql_using="hash",
    )
    op.create_index(
        "api_keys_user_id_idx",
        "api_keys",
        ["user_id"],
        unique=False,
        postgresql_using="btree",
    )
    op.create_unique_constraint("alias_user_id_uc", "api_keys", ["alias", "user_id"])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("api_keys")
    # ### end Alembic commands ###
