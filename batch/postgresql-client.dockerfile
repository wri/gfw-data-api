FROM python:3.8-slim

# Update repos and install dependencies
RUN apt-get update \
  && apt-get --no-install-recommends -y install postgresql-client jq curl \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN pip install \
        csvkit~=1.0.5 \
        awscli~=1.18.74 \
        boto3~=1.13.24 \
        awscli-plugin-endpoint~=0.3 \
        click~=7.1.2 \
        psycopg2-binary~=2.8.5

# Copy scripts
COPY ./batch/scripts/ /opt/scripts/
COPY ./batch/python/ /opt/python/

ENV PATH="/opt/scripts:${PATH}"
ENV PATH="/opt/python:${PATH}"

WORKDIR /tmp

ENTRYPOINT ["/opt/scripts/report_status.sh"]