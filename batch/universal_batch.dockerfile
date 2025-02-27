FROM ghcr.io/osgeo/gdal:ubuntu-full-3.9.3
LABEL desc="Docker image with ALL THE THINGS for use in Batch by the GFW data API"
LABEL version="v1.0"

ENV TIPPECANOE_VERSION=2.75.1

ENV VENV_DIR="/.venv"

RUN apt-get update -y \
    && apt-get install --no-install-recommends -y python3 python-dev-is-python3 python3-venv \
        postgresql-client jq curl libsqlite3-dev zlib1g-dev zip libpq-dev build-essential gcc g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# --system-site-packages is needed to copy the GDAL Python libs into the venv
RUN python -m venv ${VENV_DIR} --system-site-packages \
    && . ${VENV_DIR}/bin/activate \
    && python -m ensurepip --upgrade \
    && python -m pip install \
        agate~=1.12.0 \
        asyncpg~=0.30.0 \
        awscli~=1.36.18 \
        awscli-plugin-endpoint~=0.4 \
        boto3~=1.35.77 \
        click~=8.1.7 \
        csvkit~=2.0.1 \
        earthengine-api~=0.1.408 \
        fiona~=1.9.6 \
        gsutil~=5.31 \
        numpy~=1.26.4 \
        pandas~=2.1.4 \
        psycopg2~=2.9.10 \
        rasterio~=1.3.11 \
        setuptools~=75.6 \
        shapely~=2.0.4 \
        SQLAlchemy~=1.3.24 \
        tileputty~=0.2.10

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