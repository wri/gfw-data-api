FROM osgeo/gdal:ubuntu-small-latest

RUN apt-get update -y && apt-get install postgresql-client-11 python3-pip jq -y

RUN pip3 install csvkit awscli fiona rasterio boto3

COPY scripts /usr/local/bin

ENTRYPOINT ["/bin/bash"]