FROM globalforestwatch/data-api-gdal:v1.1.11

# Copy scripts
COPY ./batch/scripts/ /opt/scripts/
COPY ./batch/python/ /opt/python/

# Make sure scripts are executable
RUN chmod +x -R /opt/scripts/
RUN chmod +x -R /opt/python/

ENV PATH="/opt/scripts:${PATH}"
ENV PATH="/opt/python:${PATH}"

RUN pip install earthengine-api

ENV WORKDIR="/tmp"

ENTRYPOINT ["/opt/scripts/report_status.sh"]