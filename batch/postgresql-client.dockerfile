FROM python:3.8-slim

# Update repos and install dependencies
RUN apt-get update \
  && apt-get --no-install-recommends -y install postgresql-client jq \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN pip install csvkit awscli boto3 awscli-plugin-endpoint click psycopg2-binary

# Copy scripts
COPY ./batch/scripts/ /opt/scripts/
COPY ./batch/python/ /opt/python/

ENV PATH="/opt/scripts:${PATH}"
ENV PATH="/opt/python:${PATH}"

WORKDIR /tmp

ENTRYPOINT ["/bin/bash"]