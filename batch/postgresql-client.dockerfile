FROM python:3.8-slim

# Update repos and install dependencies
RUN apt-get update \
  && apt-get -y install postgresql-client-11

RUN pip install csvkit awscli boto3

# Copy scripts
WORKDIR /usr/local/bin
COPY scripts/vector_tile_cache.sh .
RUN chmod +x vector_tile_cache.sh

RUN mkdir -p /usr/local/data
WORKDIR /usr/local/data

ENTRYPOINT ["vector_tile_cache.sh"]