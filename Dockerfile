FROM ubuntu:noble AS build

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
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_INSTALL_DIR="/usr/local/bin" \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    VIRTUAL_ENV=/app/.venv

# Make sure that the virtual environment is in the PATH so
# we can use the binaries of packages that we install such as pip
# without needing to activate the virtual environment explicitly
ENV PATH=$VIRTUAL_ENV/bin:/usr/local/bin:$PATH

# Create a virtual environment with uv inside the container
RUN curl -LsSf https://astral.sh/uv/0.5.24/install.sh | sh && \
    uv venv $VIRTUAL_ENV --python 3.10 --seed

# Copy pyproject.toml and uv.lock to a temporary directory and install
# dependencies into the venv
COPY pyproject.toml /_lock/
COPY uv.lock /_lock/
RUN cd /_lock && \
    uv sync --locked --no-install-project

# Start the runtime stage
FROM ubuntu:noble
SHELL ["sh", "-exc"]

ENV TZ=UTC
RUN echo $TZ > /etc/timezone

RUN apt-get update -qy && \
    apt-get install -qyy \
        -o APT::Install-Recommends=false \
        -o APT::Install-Suggests=false \
        expat \
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

COPY --chmod=777 app/settings/prestart.sh /app/prestart.sh
COPY --chmod=777 app/settings/start.sh /app/start.sh

COPY ./app /app/app

WORKDIR /app
