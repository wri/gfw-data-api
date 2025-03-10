ARG ENV
ARG PYTHON_VERSION="3.10"
ARG USR_LOCAL_BIN=/usr/local/bin
ARG UV_VERSION="0.5.63"
ARG VENV_DIR=/app/.venv

FROM ubuntu:noble AS build

ARG ENV
ARG PYTHON_VERSION
ARG USR_LOCAL_BIN
ARG UV_VERSION
ARG VENV_DIR

RUN apt-get update -qy && \
    apt-get install -qyy \
        -o APT::Install-Recommends=false \
        -o APT::Install-Suggests=false \
        ca-certificates \
        clang \
        curl \
        gcc \
        git \
        libgdal-dev \
        libpq-dev \
        make

# Set uv env variables for behavior and venv directory
ENV PATH=${USR_LOCAL_BIN}:${PATH} \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=${VENV_DIR} \
    UV_UNMANAGED_INSTALL=${USR_LOCAL_BIN}

# Create a virtual environment with uv inside the container
RUN curl -LsSf https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-installer.sh | sh && \
    uv venv ${VENV_DIR} --python ${PYTHON_VERSION} --seed

# Copy pyproject.toml and uv.lock to a temporary directory and install
# dependencies into the venv
COPY pyproject.toml /_lock/
COPY uv.lock /_lock/
RUN if [ "$ENV" = "dev" ] || [ "$ENV" = "test" ]; then \
        echo "Install all dependencies" && \
        cd /_lock && \
        uv sync --locked --no-install-project --dev; \
    else \
        echo "Install production dependencies only" && \
        cd /_lock && \
        uv sync --locked --no-install-project --no-dev; \
    fi


# Start the runtime stage
FROM ubuntu:noble

ARG USR_LOCAL_BIN
ARG VENV_DIR

SHELL ["sh", "-exc"]

ENV PATH=${VENV_DIR}/bin:${USR_LOCAL_BIN}:${PATH}
ENV TZ=UTC
ENV VENV_DIR=${VENV_DIR}

RUN echo $TZ > /etc/timezone

RUN apt-get update -qy && \
    apt-get install -qyy \
        -o APT::Install-Recommends=false \
        -o APT::Install-Suggests=false \
        expat \
        jq \
        libgdal-dev \
        postgresql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists && \
    rm -rf /var/cache/apt

COPY --chmod=777 wait_for_postgres.sh /usr/local/bin/wait_for_postgres.sh

# Set the entry point and signal handling
ENTRYPOINT [ "/app/start.sh" ]
STOPSIGNAL SIGINT

# Copy the pre-built `/app` directory from the build stage
COPY --from=build --chmod=777 /app /app
COPY --from=build --chmod=777 /root /root

COPY newrelic.ini /app/newrelic.ini
COPY alembic.ini /app/alembic.ini

COPY --chmod=777 app/settings/gunicorn_conf.py /app/gunicorn_conf.py
COPY --chmod=777 app/settings/prestart.sh /app/prestart.sh
COPY --chmod=777 app/settings/start.sh /app/start.sh

COPY ./app /app/app

WORKDIR /app
