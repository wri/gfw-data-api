"""empty message.

Revision ID: bf6a4909ff4d
Revises: 1409885acd03
Create Date: 2021-04-23 16:36:42.542146
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bf6a4909ff4d"  # pragma: allowlist secret
down_revision = "1409885acd03"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("api_keys", sa.Column("nick_name", sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("api_keys", "nick_name")
    # ### end Alembic commands ###
