FROM globalforestwatch/pixetl:v1.5.3

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

# DEBUGGING: Compile libleak
RUN apt install -y git && cd $WORKDIR && git clone --recursive https://github.com/WuBingzheng/libleak.git && cd libleak && make

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ENTRYPOINT ["/opt/scripts/report_status.sh"]