"""Drop reset_latest Trigger, add is_downloadable columns to datasets, versions
and assets and populate fields.

Revision ID: 4763f4b8141a
Revises: d62a9b15f844
Create Date: 2021-07-13 01:10:24.418512
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.


revision = "4763f4b8141a"  # pragma: allowlist secret
down_revision = "d62a9b15f844"  # pragma: allowlist secret
branch_labels = None
depends_on = None

tables = ["datasets", "versions", "assets"]
column_name = "is_downloadable"


def upgrade():
    op.execute("""DROP FUNCTION public.reset_latest() CASCADE;""")

    for table_name in tables:
        op.add_column(table_name, sa.Column(column_name, sa.Boolean(), nullable=True))

        op.execute(
            f"""UPDATE {table_name}
                    SET {column_name} = true
                    WHERE {column_name} IS NULL;
                """
        )
        op.alter_column(table_name, column_name, nullable=False)


def downgrade():
    for table_name in tables:
        op.drop_column(table_name, column_name)

    ### Create custom triggers
    op.execute(
        """
        CREATE FUNCTION public.reset_latest()
            RETURNS trigger
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE NOT LEAKPROOF
        AS $BODY$
                BEGIN
                    IF NEW.is_latest = true THEN
                        UPDATE versions
                          SET is_latest = false
                            WHERE versions.dataset = NEW.dataset
                             AND versions.version <> NEW.version;
                    END IF;

                RETURN NEW;
                END;
               $BODY$;"""
    )

    op.execute(
        """
        CREATE TRIGGER latest_version
            BEFORE INSERT OR UPDATE
            ON public.versions
            FOR EACH ROW
            EXECUTE PROCEDURE public.reset_latest();
        """
    )
