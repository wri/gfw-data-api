FROM osgeo/gdal:ubuntu-small-latest

RUN apt-get update -y \
    && apt-get install --no-install-recommends -y postgresql-client-12 python3-pip jq \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install csvkit awscli fiona rasterio boto3 awscli-plugin-endpoint

# Copy scripts
COPY ./batch/scripts/ /opt/scripts/
COPY ./batch/python/ /opt/python/

ENV PATH="/opt/scripts:${PATH}"
ENV PATH="/opt/python:${PATH}"

WORKDIR /tmp

ENTRYPOINT ["/bin/bash"]