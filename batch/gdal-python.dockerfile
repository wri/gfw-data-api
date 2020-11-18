FROM globalforestwatch/data-api-gdal:1.1.2

# Copy scripts
COPY ./batch/scripts/ /opt/scripts/
COPY ./batch/python/ /opt/python/

# Make sure scripts are executable
RUN chmod +x -R /opt/scripts/
RUN chmod +x -R /opt/python/

ENV PATH="/opt/scripts:${PATH}"
ENV PATH="/opt/python:${PATH}"

ENV SRC_PATH="/usr/src/build_rgb"

RUN mkdir -p $SRC_PATH
COPY ./batch/cpp/build_rgb.cpp $SRC_PATH

# Compile build_rgb for use with GLAD/RADD raster tile caches
RUN cd $SRC_PATH && g++ `gdal-config --cflags` build_rgb.cpp -o /usr/bin/build_rgb `gdal-config --libs`

ENV WORKDIR="/tmp"

ENTRYPOINT ["/opt/scripts/report_status.sh"]