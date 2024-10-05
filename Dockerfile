# Use a multi-stage build to first get uv
FROM ghcr.io/astral-sh/uv:0.4.18 as uv

FROM ubuntu:noble as build

RUN apt-get update -qy && \
    apt-get install -qyy \
        -o APT::Install-Recommends=false \
        -o APT::Install-Suggests=false \
        ca-certificates \
        git \
        make \
        clang \
        libpq-dev

# We need to set this environment variable so that uv knows where
# the virtual environment is to install packages
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    VIRTUAL_ENV=/app/.venv

#RUN mkdir -p /app

# Create a user and group for the application
#RUN groupadd -r app && \
#    useradd -r -d /app -g app -N app
#
#RUN chown -R app:app /app
#
#USER app

# Create a virtual environment with uv inside the container
RUN --mount=type=cache,target=/app/.cache \
    --mount=from=uv,source=/uv,target=./uv \
    /uv python install 3.10 && \
    /uv venv $VIRTUAL_ENV

# Make sure that the virtual environment is in the PATH so
# we can use the binaries of packages that we install such as pip
# without needing to activate the virtual environment explicitly
ENV PATH=$VIRTUAL_ENV/bin:/usr/local/bin:$PATH

RUN --mount=type=cache,target=/app/.cache \
    --mount=from=uv,source=/uv,target=./uv \
    /uv pip install setuptools wheel

# Copy pyproject.toml and uv.lock to a temporary directory
COPY pyproject.toml /_lock/
COPY uv.lock /_lock/

# Install the packages with uv using --mount=type=cache to cache the downloaded packages
RUN --mount=type=cache,target=/app/.cache \
    --mount=from=uv,source=/uv,target=./uv \
    cd /_lock && \
    /uv sync --locked --no-install-project

# Start the runtime stage
FROM ubuntu:noble
SHELL ["sh", "-exc"]

ENV PATH=$VIRTUAL_ENV/bin:/usr/local/bin:$PATH

RUN apt-get update -qy && \
    apt-get install -qyy \
        -o APT::Install-Recommends=false \
        -o APT::Install-Suggests=false \
        postgresql-client \
        expat

# Create a user and group for the application
#RUN groupadd -r app && \
#    useradd -r -d /app -g app -N app

COPY --chmod=777 wait_for_postgres.sh /usr/local/bin/wait_for_postgres.sh

# Set the entry point and signal handling
ENTRYPOINT [ "/app/start.sh" ]
#ENTRYPOINT [ "/bin/bash" ]
STOPSIGNAL SIGINT

# Copy the pre-built `/app` directory from the build stage
COPY --from=build --chmod=777 /app /app
COPY --from=build --chmod=777 /root /root

COPY newrelic.ini /app/newrelic.ini
COPY alembic.ini /app/alembic.ini

COPY --chmod=777 app/settings/prestart.sh /app/prestart.sh
COPY --chmod=777 app/settings/start.sh /app/start.sh

# If your application is NOT a proper Python package that got
# pip-installed above, you need to copy your application into
# the container HERE:
COPY ./app /app/app

#USER app
WORKDIR /app
