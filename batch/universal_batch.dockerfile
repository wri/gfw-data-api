FROM ghcr.io/osgeo/gdal:ubuntu-full-3.9.3
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

# --system-site-packages is needed to copy the GDAL Python libs into the venv
RUN uv venv ${VENV_DIR} --system-site-packages

# Install Python dependencies from the locked, batch-specific pyproject.toml.
# Kept separate from the main Data API's pyproject.toml/uv.lock, since this
# image's dependency set is intentionally standalone (see batch/pyproject.toml).
COPY ./batch/pyproject.toml ./batch/uv.lock /opt/batch-deps/
RUN cd /opt/batch-deps && uv sync --locked

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