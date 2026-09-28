FROM globalforestwatch/pixetl:v1.9.0b5

# report_status.sh uses curl to report AWS Batch job status back to the API.
# Install it explicitly rather than relying on the upstream PixETL image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Copy scripts
COPY ./batch/scripts/ /opt/scripts/
COPY ./batch/python/ /opt/python/

RUN ln -f -s /usr/bin/python3 /usr/bin/python

# make sure scripts are executable
RUN chmod +x -R /opt/scripts/
RUN chmod +x -R /opt/python/

ENV PATH="/opt/scripts:${PATH}"
ENV PATH="/opt/python:${PATH}"

WORKDIR /tmp

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ENTRYPOINT ["/opt/scripts/report_status.sh"]
