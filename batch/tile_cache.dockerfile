FROM globalforestwatch/data-api-tippecanoe:1.1.3

# Copy scripts
COPY ./batch/scripts/ /opt/scripts/
COPY ./batch/python/ /opt/python/

# make sure scripts are excecutable
RUN chmod +x -R /opt/scripts/
RUN chmod +x -R /opt/python/

ENV PATH="/opt/scripts:${PATH}"
ENV PATH="/opt/python:${PATH}"

WORKDIR /tmp

ENTRYPOINT ["/opt/scripts/report_status.sh"]