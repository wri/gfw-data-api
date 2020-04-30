"""empty message

Revision ID: 86ae41de358d
Revises:
Create Date: 2020-04-14 21:58:38.173605

"""
import json
import os

import boto3
import sqlalchemy as sa
import geoalchemy2

from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "e47ec2fc3c51"
down_revision = None
branch_labels = None
depends_on = None

if os.environ["ENV"] == "docker":
    USERNAME = os.environ["DB_USER"]
    PASSWORD = os.environ["DB_PASSWORD"]
    DBNAME = os.environ["DATABASE"]
else:
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=os.environ["SECRET_NAME"])
    secrets = json.loads(response["SecretString"])

    USERNAME = secrets["username"]
    PASSWORD = secrets["password"]
    DBNAME = secrets["dbname"]


def upgrade():

    op.execute(f"""CREATE EXTENSION postgis;""")

    #### Create read only user
    op.execute(
        f"""
                DO
                $do$
                BEGIN
                   IF NOT EXISTS (
                      SELECT                       -- SELECT list can stay empty for this
                      FROM   pg_catalog.pg_roles
                      WHERE  rolname = '{USERNAME}') THEN
                      CREATE ROLE {USERNAME} LOGIN PASSWORD '{PASSWORD}';
                   END IF;
                END
                $do$;
                """
    )
    op.execute(f"GRANT CONNECT ON DATABASE {DBNAME} TO {USERNAME};")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {USERNAME};"
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
                            WHEN fishnet.top_1 < 0::double precision THEN 'S'::text || lpad((fishnet.top_1 * '-1'::integer::double precision)::text, 2, '0'::text)
                            WHEN fishnet.top_1 = 0::double precision THEN 'N'::text || lpad((fishnet.top_1 * '-1'::integer::double precision)::text, 2, '0'::text)
                            ELSE 'N'::text || lpad(fishnet.top_1::text, 2, '0'::text)
                        END AS top_1,
                        CASE
                            WHEN fishnet.left_1 < 0::double precision THEN 'W'::text || lpad((fishnet.left_1 * '-1'::integer::double precision)::text, 3, '0'::text)
                            ELSE 'E'::text || lpad(fishnet.left_1::text, 3, '0'::text)
                        END AS left_1,
                        CASE
                            WHEN fishnet.top_10 < 0::double precision THEN 'S'::text || lpad((fishnet.top_10 * '-1'::integer::double precision)::text, 2, '0'::text)
                            WHEN fishnet.top_10 = 0::double precision THEN 'N'::text || lpad((fishnet.top_10 * '-1'::integer::double precision)::text, 2, '0'::text)
                            ELSE 'N'::text || lpad(fishnet.top_10::text, 2, '0'::text)
                        END AS top_10,
                        CASE
                            WHEN fishnet.left_10 < 0::double precision THEN 'W'::text || lpad((fishnet.left_10 * '-1'::integer::double precision)::text, 3, '0'::text)
                            ELSE 'E'::text || lpad(fishnet.left_10::text, 3, '0'::text)
                        END AS left_10,
                    fishnet.geom
                   FROM fishnet
                )
         SELECT grid.top_1 || grid.left_1 AS gfw_grid_1x1_id,
            grid.top_10 || grid.left_10 AS gfw_grid_10x10_id,
            grid.geom
           FROM grid
        WITH DATA;

        CREATE INDEX IF NOT EXISTS gfw_grid_1x1_geom_idx
            ON public.gfw_grid_1x1 USING gist
            (geom);"""
    )

    #### Create custom data type gfw_grid_type
    op.execute(
        """
    CREATE TYPE public.gfw_grid_type AS (gfw_grid_1x1 text, gfw_grid_10x10 text, geom geometry);
    """
    )

    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "datasets",
        sa.Column(
            "created_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column(
            "updated_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column("dataset", sa.String(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("dataset"),
    )

    op.create_table(
        "geostore",
        sa.Column(
            "created_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column(
            "updated_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column("gfw_geostore_id", postgresql.UUID(), nullable=False),
        sa.Column("gfw_area__ha", sa.Numeric(), nullable=False),
        sa.Column(
            "gfw_bbox",
            geoalchemy2.types.Geometry(geometry_type="POLYGON", srid=4326),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("gfw_geostore_id"),
    )

    op.create_index(
        "geostore_gfw_geostore_id_idx",
        "geostore",
        ["gfw_geostore_id"],
        unique=False,
        postgresql_using="hash",
    )

    op.create_table(
        "versions",
        sa.Column(
            "created_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column(
            "updated_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column("dataset", sa.String(), nullable=False),
        sa.Column("version", sa.String(), nullable=False),
        sa.Column("is_latest", sa.Boolean(), nullable=True),
        sa.Column("source_type", sa.String(), nullable=False),
        sa.Column("has_vector_tile_cache", sa.Boolean(), nullable=True),
        sa.Column("has_raster_tile_cache", sa.Boolean(), nullable=True),
        sa.Column("has_geostore", sa.Boolean(), nullable=True),
        sa.Column("has_feature_info", sa.Boolean(), nullable=True),
        sa.Column("has_10_40000_tiles", sa.Boolean(), nullable=True),
        sa.Column("has_90_27008_tiles", sa.Boolean(), nullable=True),
        sa.Column("has_90_9876_tiles", sa.Boolean(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(["dataset"], ["datasets.dataset"], name="fk"),
        sa.PrimaryKeyConstraint("dataset", "version"),
    )

    op.create_table(
        "assets",
        sa.Column(
            "created_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column(
            "updated_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column("dataset", sa.String(), nullable=False),
        sa.Column("version", sa.String(), nullable=False),
        sa.Column("asset_type", sa.String(), nullable=False),
        sa.Column("asset_uri", sa.String(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(
            ["dataset", "version"], ["versions.dataset", "versions.version"], name="fk"
        ),
        sa.PrimaryKeyConstraint("dataset", "version", "asset_type"),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("assets")
    op.drop_table("versions")
    op.drop_index("geostore_gfw_geostore_id_idx", table_name="geostore")
    op.drop_table("geostore")
    op.drop_table("datasets")
    # ### end Alembic commands ###

    op.execute("""DROP TYPE IF EXISTS public.gfw_grid_type;""")
    op.execute("""DROP MATERIALIZED VIEW IF EXISTS public.gfw_grid_1x1;""")
    op.execute("""DROP FUNCTION IF EXISTS public.gfw_create_fishnet;""")
    op.execute(f"""DROP USER IF EXISTS {USERNAME}""")
