FROM globalforestwatch/pixetl:latest

# Copy scripts
COPY ./batch/scripts/ /opt/scripts/
COPY ./batch/python/ /opt/python/

# make sure scripts are executable
RUN chmod +x -R /opt/scripts/
RUN chmod +x -R /opt/python/

RUN ln -s /usr/bin/python3 /usr/bin/python

ENV PATH="/opt/scripts:${PATH}"
ENV PATH="/opt/python:${PATH}"

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

WORKDIR /tmp

ENTRYPOINT ["/opt/scripts/report_status.sh"]