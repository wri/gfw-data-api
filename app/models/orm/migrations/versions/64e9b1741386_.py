"""Update 1x1 grid materialized view.

Revision ID: 64e9b1741386
Revises: 4a4df534a8f6
Create Date: 2020-07-30 13:46:39.029933
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "64e9b1741386"  # pragma: allowlist secret
down_revision = "4a4df534a8f6"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute(
        """
    DROP MATERIALIZED VIEW public.gfw_grid_1x1;
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
         SELECT grid.top_1 || '_' || grid.left_1 AS gfw_grid_1x1_id,
            grid.top_10 || '_' || grid.left_10 AS gfw_grid_10x10_id,
            grid.geom
           FROM grid
        WITH DATA;

        CREATE INDEX IF NOT EXISTS gfw_grid_1x1_geom_idx
            ON public.gfw_grid_1x1 USING gist
            (geom);"""
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute(
        """
        DROP MATERIALIZED VIEW public.gfw_grid_1x1;
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
    # ### end Alembic commands ###
