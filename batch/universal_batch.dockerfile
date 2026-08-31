FROM ghcr.io/osgeo/gdal:ubuntu-full-3.12.4
LABEL desc="Docker image with ALL THE THINGS for use in Batch by the GFW data API"
LABEL version="v1.1"

ENV TIPPECANOE_VERSION=2.75.1

ENV VENV_DIR="/.venv"

# The base image ships an Apache Arrow apt source whose signing key is no
# longer fetchable by apt-key; drop it since we don't need Arrow packages here.
RUN rm -f /etc/apt/sources.list.d/apache-arrow.sources

RUN apt-get update -y \
    && apt-get install --no-install-recommends -y python3 python-dev-is-python3 \
        postgresql-client jq curl libsqlite3-dev zlib1g-dev zip libpq-dev build-essential gcc g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=${VENV_DIR}

# --system-site-packages is needed to copy the GDAL Python libs into the venv.
# The base image (and anything installed into it) also ships its own
# apt-level numpy/GEOS/etc for those GDAL bindings, so the venv's own
# packages MUST always shadow the system ones deterministically -- see the
# PATH/PYTHONPATH pinning and the build-time assertion below, both of which
# are what actually guarantee this (not just --system-site-packages' default
# sys.path ordering, which can be silently overridden by an inherited
# PYTHONPATH).
RUN uv venv ${VENV_DIR} --system-site-packages

# Pin the venv onto PATH and mark it as the active interpreter at the image
# level -- not just inside report_status.sh's runtime `. activate` -- so
# every script, shell, or entrypoint that ever runs in this image resolves
# `python`/`python3` to the venv first, deterministically, regardless of how
# it's invoked. Also clear PYTHONPATH so nothing the base image (or a later
# layer) sets can be prepended ahead of the venv on sys.path.
ENV VIRTUAL_ENV=${VENV_DIR} \
    PATH="${VENV_DIR}/bin:${PATH}" \
    PYTHONPATH=""

# Install Python dependencies from the locked, batch-specific pyproject.toml.
# Kept separate from the main Data API's pyproject.toml/uv.lock, since this
# image's dependency set is intentionally standalone (see batch/pyproject.toml).
COPY ./batch/pyproject.toml ./batch/uv.lock /opt/batch-deps/
RUN cd /opt/batch-deps && uv sync --locked

# Fail the build (loudly, at build time) rather than shipping an image where
# numpy/pandas/shapely/rasterio silently resolve to the base image's
# apt-level copies instead of the pinned, uv-locked ones. This is the actual
# guarantee that "uv's packages shadow system ones" -- everything above just
# makes it *likely*; this makes it *verified*.
RUN python3 -c "\
import numpy, pandas, shapely, rasterio; \
mods = {'numpy': numpy, 'pandas': pandas, 'shapely': shapely, 'rasterio': rasterio}; \
bad = {n: m.__file__ for n, m in mods.items() if not m.__file__.startswith('${VENV_DIR}')}; \
assert not bad, f'Packages resolved outside {\"${VENV_DIR}\"}, system packages are shadowing the venv: {bad}'; \
print('OK: numpy/pandas/shapely/rasterio all resolve from the venv:'); \
[print(f'  {n}: {m.__file__} ({m.__version__})') for n, m in mods.items()]"

# Install TippeCanoe
RUN mkdir -p /opt/src
WORKDIR /opt/src
RUN curl https://codeload.github.com/felt/tippecanoe/tar.gz/${TIPPECANOE_VERSION} | tar -xz \
    && cd /opt/src/tippecanoe-${TIPPECANOE_VERSION} \
    && make \
    && make install \
    && rm -R /opt/src/tippecanoe-${TIPPECANOE_VERSION}

# Copy scripts
COPY ./batch/scripts/ /opt/scripts/
COPY ./batch/python/ /opt/python/

# Make sure scripts are executable
RUN chmod +x -R /opt/scripts/
RUN chmod +x -R /opt/python/

ENV PATH="/opt/scripts:${PATH}"
ENV PATH="/opt/python:${PATH}"

ENV WORKDIR="/"
WORKDIR /

ENTRYPOINT ["/opt/scripts/report_status.sh"]