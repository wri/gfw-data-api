FROM osgeo/gdal:ubuntu-small-latest

RUN apt-get update -y && apt-get install postgresql-client-12 python3-pip -y

RUN pip3 install csvkit awscli fiona rasterio boto3

COPY scripts /usr/local/bin

ENTRYPOINT ["/bin/bash"]