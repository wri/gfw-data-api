# Multi-arch (amd64 + arm64) PostgreSQL 14 + PostGIS test-database image.
#
# The image this replaced, postgis/postgis:14-3.4-alpine, has no arm64 build
# at any tag -- checked directly against the Docker Hub registry API (every
# 14.x/15.x/16.x tag, both Alpine and Debian variants, linux/amd64 only). On
# a GitHub Actions arm64 runner it only ran under QEMU emulation, and
# Postgres is close to a worst case for that: emulating a real database
# doing lots of syscalls/fsync/shared-memory work is much slower than
# emulating typical CPU-bound application code.
#
# The official `postgres` image, unlike postgis/postgis, IS a genuine
# multi-arch Docker Official Image. This installs PostGIS on top of it via
# the PGDG (PostgreSQL Global Development Group) apt repository -- the same
# repository postgis/postgis's own Debian variant is ultimately built from --
# which currently publishes PostGIS 3.4.1 for Postgres 14 on bookworm,
# matching the "14-3.4" this project already pinned to, and explicitly
# including arm64 (unlike Debian's own default archive, whose bookworm
# PostGIS package lags behind and isn't guaranteed to track a specific
# PostGIS minor version).
#
# App migrations (app/models/orm/migrations/versions/86ae41de358d_.py) already
# run `CREATE EXTENSION IF NOT EXISTS postgis;` themselves, so this image
# only needs the extension's files installed on disk -- no custom
# docker-entrypoint-initdb.d script needed.
FROM postgres:14-bookworm

# Historical note: some past postgres:14-bookworm image builds hit
# "NO_PUBKEY" errors on `apt-get update` against Debian's own archive due to
# a keyring rotation (github.com/docker-library/postgres/issues/1100). That
# was specific to mid-2023 image layers and should be long resolved by now,
# but if it resurfaces, `apt-get install --reinstall debian-archive-keyring`
# before this RUN step is the known fix.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
         --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
         > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
         postgresql-14-postgis-3 \
         postgresql-14-postgis-3-scripts \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*
