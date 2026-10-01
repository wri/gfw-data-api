# Multi-arch (amd64 + arm64) PostgreSQL + PostGIS image, used by
# docker-compose.{dev,prod,test}.yml.
#
# This project only ever runs one PostgreSQL version at a time, so there's
# a single Dockerfile here (not one per version) -- to upgrade, change
# PG_VERSION below and rebuild; nothing else in this file should need to
# change. Double check the PostGIS version comment further down still
# holds for the new PG_VERSION (PGDG's currently published PostGIS build
# can differ across postgres major versions).
#
# The image this replaced everywhere, postgis/postgis:14-3.4-alpine, has no
# arm64 build at any tag -- checked directly against the Docker Hub
# registry API (every 14.x/15.x/16.x tag, both Alpine and Debian variants,
# linux/amd64 only). Under emulation (e.g. on Apple Silicon or a GitHub
# Actions arm64 runner) Postgres is close to a worst case: emulating a real
# database doing lots of syscalls/fsync/shared-memory work is much slower
# than emulating typical CPU-bound application code.
#
# The official `postgres` image, unlike postgis/postgis, IS a genuine
# multi-arch Docker Official Image. This installs PostGIS on top of it via
# the PGDG (PostgreSQL Global Development Group) apt repository -- the same
# repository postgis/postgis's own Debian variant is ultimately built from.
# As of PG_VERSION=14, PGDG currently publishes PostGIS 3.4.1 for Postgres
# 14 on bookworm, explicitly including arm64 (unlike Debian's own default
# archive, whose bookworm PostGIS package lags behind and isn't guaranteed
# to track a specific PostGIS minor version).
#
# App migrations (app/models/orm/migrations/versions/86ae41de358d_.py) already
# run `CREATE EXTENSION IF NOT EXISTS postgis;` themselves, so this image
# only needs the extension's files installed on disk -- no custom
# docker-entrypoint-initdb.d script needed.
ARG PG_VERSION=14

FROM postgres:${PG_VERSION}-bookworm

# Re-declared after FROM -- an ARG before FROM is only in scope for the
# FROM line itself; this makes the same value available below too.
ARG PG_VERSION

# Historical note: some past postgres:*-bookworm image builds hit
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
         postgresql-${PG_VERSION}-postgis-3 \
         postgresql-${PG_VERSION}-postgis-3-scripts \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*
