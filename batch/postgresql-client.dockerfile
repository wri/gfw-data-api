FROM python:3.8-slim

# Update repos and install dependencies
RUN apt-get update \
  && apt-get -y install postgresql-client jq

RUN pip install csvkit awscli boto3

# Copy scripts
COPY ./batch/scripts/ /usr/local/bin/

ENTRYPOINT ["/bin/bash"]