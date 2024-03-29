"""Update asset creation option records.

Revision ID: 73fb3f5e39b8
Revises: 167eebbf29e4
Create Date: 2021-01-13 21:18:04.313795
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "73fb3f5e39b8"  # pragma: allowlist secret
down_revision = "167eebbf29e4"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    # change indices to column_names instead of column_name, and use array
    op.execute(
        """UPDATE assets
                SET creation_options = jsonb_set(creation_options, '{indices}', REGEXP_REPLACE((creation_options->'indices')::text, '"column_name": ("[_[:alpha:]]*")', '"column_names": [\\1]', 'g')::jsonb, false)
                WHERE (asset_type = 'Geo database table' or asset_type = 'Database table') AND creation_options ? 'indices';
        """
    )

    # change cluster to column_names instead of column_name, and use array
    op.execute(
        """UPDATE assets
                SET creation_options = jsonb_set(creation_options, '{cluster}', REGEXP_REPLACE((creation_options->'cluster')::text, '"column_name": ("[_[:alpha:]]*")', '"column_names": [\\1]', 'g')::jsonb, false)
                WHERE (asset_type = 'Geo database table' or asset_type = 'Database table') AND creation_options ? 'cluster';
        """
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    # change back indices
    op.execute(
        """UPDATE assets
                SET creation_options = jsonb_set(creation_options, '{indices}', REGEXP_REPLACE((creation_options->'indices')::text, '"column_names": \[("[_[:alpha:]]*")\]', '"column_name": \\1', 'g')::jsonb, false)
                WHERE (asset_type = 'Geo database table' or asset_type = 'Database table') AND creation_options ? 'indices';
        """
    )

    # change back cluster
    op.execute(
        """UPDATE assets
                SET creation_options = jsonb_set(creation_options, '{cluster}', REGEXP_REPLACE((creation_options->'cluster')::text, '"column_names": \[("[_[:alpha:]]*")\]', '"column_name": \\1', 'g')::jsonb, false)
                WHERE (asset_type = 'Geo database table' or asset_type = 'Database table') AND creation_options ? 'cluster';
        """
    )

    # ### end Alembic commands ###
