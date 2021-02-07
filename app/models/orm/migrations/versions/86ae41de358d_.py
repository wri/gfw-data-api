"""Create custom extensions, functions, views, types and roles.

Revision ID: 86ae41de358d
Revises:
Create Date: 2020-04-14 21:58:38.173605
"""
from alembic import op

from app.settings.globals import READER_DBNAME, READER_PASSWORD, READER_USERNAME

# revision identifiers, used by Alembic.
revision = "e47ec2fc3c51"  # pragma: allowlist secret
down_revision = None
branch_labels = None
depends_on = None


def upgrade():

    # Create extensions
    op.execute("""CREATE EXTENSION IF NOT EXISTS postgis;""")
    op.execute("""CREATE EXTENSION IF NOT EXISTS "uuid-ossp";""")

    #### Create read only user
    op.execute(
        f"""
                DO
                $do$
                BEGIN
                   IF NOT EXISTS (
                      SELECT                       -- SELECT list can stay empty for this
                      FROM   pg_catalog.pg_roles
                      WHERE  rolname = '{READER_USERNAME}') THEN
                      CREATE ROLE {READER_USERNAME} LOGIN PASSWORD '{READER_PASSWORD}';
                   END IF;
                END
                $do$;
                """
    )
    op.execute(f"GRANT CONNECT ON DATABASE {READER_DBNAME} TO {READER_USERNAME};")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {READER_USERNAME};"
    )

    #### Create Fishnet Function
    #### https://gis.stackexchange.com/questions/16374/creating-regular-polygon-grid-in-postgis
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.gfw_create_fishnet(
            nrow integer,
            ncol integer,
            xsize double precision,
            ysize double precision,
            x0 double precision DEFAULT 0,
            y0 double precision DEFAULT 0,
            OUT "row" integer,
            OUT col integer,
            OUT geom geometry)
            RETURNS SETOF record
            LANGUAGE 'sql'

            COST 100
            IMMUTABLE STRICT
            ROWS 1000
        AS $BODY$
                SELECT i + 1 AS row, j + 1 AS col, ST_Translate(cell, j * $3 + $5, i * $4 + $6) AS geom
                FROM generate_series(0, $1 - 1) AS i,
                     generate_series(0, $2 - 1) AS j,
                (
                SELECT ('POLYGON((0 0, 0 '||$4||', '||$3||' '||$4||', '||$3||' 0,0 0))')::geometry AS cell
                ) AS foo;
                $BODY$;
    """
    )

    #### Create 1x1 degree grid as materialized view
    op.execute(
        """
        CREATE MATERIALIZED VIEW public.gfw_grid_1x1
        TABLESPACE pg_default
        AS
         WITH fishnet AS (
                 SELECT st_x(st_centroid(gfw_create_fishnet.geom)) - 0.5::double precision AS left_1,
                    st_y(st_centroid(gfw_create_fishnet.geom)) + 0.5::double precision AS top_1,
                    floor(st_x(st_centroid(gfw_create_fishnet.geom)) / 10::double precision) * 10::double precision AS left_10,
                    ceil(st_y(st_centroid(gfw_create_fishnet.geom)) / 10::double precision) * 10::double precision AS top_10,
                    st_setsrid(gfw_create_fishnet.geom, 4326) AS geom
                   FROM gfw_create_fishnet(180, 360, 1::double precision, 1::double precision, '-180'::integer::double precision, '-90'::integer::double precision) gfw_create_fishnet("row", col, geom)
                ), grid AS (
                 SELECT
                        CASE
                            WHEN fishnet.top_1 < 0::double precision THEN lpad((fishnet.top_1 * '-1'::integer::double precision)::text, 2, '0'::text) || 'S'::text
                            WHEN fishnet.top_1 = 0::double precision THEN lpad((fishnet.top_1 * '-1'::integer::double precision)::text, 2, '0'::text) || 'N'::text
                            ELSE lpad(fishnet.top_1::text, 2, '0'::text) || 'N'::text
                        END AS top_1,
                        CASE
                            WHEN fishnet.left_1 < 0::double precision THEN lpad((fishnet.left_1 * '-1'::integer::double precision)::text, 3, '0'::text) || 'W'::text
                            ELSE lpad(fishnet.left_1::text, 3, '0'::text) || 'E'::text
                        END AS left_1,
                        CASE
                            WHEN fishnet.top_10 < 0::double precision THEN lpad((fishnet.top_10 * '-1'::integer::double precision)::text, 2, '0'::text) || 'S'::text
                            WHEN fishnet.top_10 = 0::double precision THEN lpad((fishnet.top_10 * '-1'::integer::double precision)::text, 2, '0'::text) || 'N'::text
                            ELSE lpad(fishnet.top_10::text, 2, '0'::text) || 'N'::text
                        END AS top_10,
                        CASE
                            WHEN fishnet.left_10 < 0::double precision THEN lpad((fishnet.left_10 * '-1'::integer::double precision)::text, 3, '0'::text) || 'W'::text
                            ELSE lpad(fishnet.left_10::text, 3, '0'::text) || 'E'::text
                        END AS left_10,
                    fishnet.geom
                   FROM fishnet
                )
         SELECT (grid.top_1 || '_'::text) || grid.left_1 AS gfw_grid_1x1_id,
            (grid.top_10 || '_'::text) || grid.left_10 AS gfw_grid_10x10_id,
            grid.geom
           FROM grid
        WITH DATA;

        CREATE INDEX gfw_grid_1x1_geom_idx
            ON public.gfw_grid_1x1 USING gist
            (geom)
            TABLESPACE pg_default;
    """
    )

    #### Create custom data type gfw_grid_type
    op.execute(
        """
        CREATE TYPE public.gfw_grid_type AS
            (
                gfw_grid_1x1 text,
                gfw_grid_10x10 text,
                geom geometry
            );
        """
    )

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

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    # Delete custom types
    op.execute("""DROP TYPE IF EXISTS public.gfw_grid_type;""")

    # Delete custom materialized views
    op.execute("""DROP MATERIALIZED VIEW IF EXISTS public.gfw_grid_1x1;""")

    # Delete custom functions
    op.execute("""DROP FUNCTION IF EXISTS public.gfw_create_fishnet;""")
    op.execute("""DROP FUNCTION IF EXISTS public.reset_latest;""")

    # Delete custom users
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM {READER_USERNAME};"
    )
    op.execute(f"REVOKE CONNECT ON DATABASE {READER_DBNAME} FROM {READER_USERNAME};")
    op.execute(f"""DROP USER IF EXISTS {READER_USERNAME};""")

    # Delete extensions
    op.execute("""DROP EXTENSION IF EXISTS "uuid-ossp";""")
    op.execute("""DROP EXTENSION IF EXISTS "postgis" CASCADE;""")
