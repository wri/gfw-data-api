FROM osgeo/gdal:ubuntu-small-latest

RUN apt-get update -y && \
    apt-get install -y postgresql-client-12 python3-pip jq

RUN pip3 install csvkit awscli fiona rasterio boto3 awscli-plugin-endpoint

# Copy scripts
COPY ./batch/scripts/ /opt/scripts/
COPY ./batch/python/ /opt/python/

ENV PATH="/opt/scripts:${PATH}"
ENV PATH="/opt/python:${PATH}"

WORKDIR /tmp

ENTRYPOINT ["/bin/bash"]