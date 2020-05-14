FROM python:3.8-slim

# Update repos and install dependencies
RUN apt-get update \
  && apt-get -y install build-essential curl libsqlite3-dev zlib1g-dev

# Install TippeCanoe
ENV TIPPECANOE_VERSION=1.35.0
RUN curl https://codeload.github.com/mapbox/tippecanoe/tar.gz/${TIPPECANOE_VERSION} | tar -xz
WORKDIR tippecanoe-${TIPPECANOE_VERSION}
RUN make && make install

# Install tileputty
RUN pip install tileputty

COPY ./batch/scripts/ /usr/local/bin/

ENTRYPOINT ["/bin/bash"]
