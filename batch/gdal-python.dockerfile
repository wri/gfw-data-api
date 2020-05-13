FROM osgeo/gdal:ubuntu-small-latest

RUN apt-get update -y && \
    apt-get install -y postgresql-client-12 python3-pip jq

RUN pip3 install csvkit awscli fiona rasterio boto3

COPY scripts /usr/local/bin

ENTRYPOINT ["/bin/bash"]