# Multi-arch (amd64 + arm64) PostgreSQL + PostGIS image, used by
# docker-compose.{dev,prod,test}.yml.
#
# postgis/postgis:14-3.4-alpine (and any other PostGIS image up to at least
# PostgreSQL 16.x, both Alpine and Debian variants) has no arm64 variant, so
# it can't be used on Apple laptops or other arm64 hosts except under
# emulation. Emulating a database doing lots of syscalls/fsync/shared-memory
# work is much slower than emulating typical CPU-bound application code,
# and this has a dramatic effect on test suite runtimes.
#
# The official `postgres` image, unlike postgis/postgis, IS multi-arch.
# This Dockerfile installs PostGIS on top of it via the PGDG
# apt repository. As of PG_VERSION=14, PGDG publishes PostGIS 3.4.1 for Postgres
# 14 on bookworm, explicitly including arm64 (unlike Debian's own default
# archive, whose bookworm PostGIS package lags behind and isn't guaranteed
# to track a specific PostGIS minor version).
ARG PG_VERSION=14

FROM postgres:${PG_VERSION}-bookworm

ARG PG_VERSION

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
