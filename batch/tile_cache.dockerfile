FROM globalforestwatch/data-api-tippecanoe:v1.3.0

# Copy scripts
COPY ./batch/scripts/ /opt/scripts/
COPY ./batch/python/ /opt/python/

# make sure scripts are executable
RUN chmod +x -R /opt/scripts/
RUN chmod +x -R /opt/python/

ENV PATH="/opt/scripts:${PATH}"
ENV PATH="/opt/python:${PATH}"

WORKDIR /tmp

ENTRYPOINT ["/opt/scripts/report_status.sh"]