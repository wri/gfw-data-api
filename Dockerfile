FROM tiangolo/uvicorn-gunicorn-fastapi:python3.10-slim

# Optional build argument for different environments
ARG ENV

ENV RYE_HOME="/opt/rye"
ENV PATH="$RYE_HOME/shims:$PATH"

RUN apt-get update -y \
    && apt-get install --no-install-recommends -y clang libc-dev \
      postgresql-client libpq-dev make git jq curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSf https://rye-up.com/get | RYE_NO_AUTO_INSTALL=1 RYE_TOOLCHAIN=/usr/bin/python RYE_INSTALL_OPTION="--yes" bash

COPY ./app /app/app
WORKDIR /app

COPY alembic.ini                /app/alembic.ini
COPY newrelic.ini               /app/newrelic.ini
COPY app/settings/prestart.sh   /app/prestart.sh
COPY app/settings/start.sh      /app/start.sh
COPY wait_for_postgres.sh       /usr/local/bin/wait_for_postgres.sh

RUN chmod +x /usr/local/bin/wait_for_postgres.sh
RUN chmod +x /app/start.sh

COPY .python-version            /app/.python-version
COPY pyproject.toml             /app/pyproject.toml
COPY README.md                  /app/README.md
COPY requirements-dev.lock      /app/requirements-dev.lock
COPY requirements.lock          /app/requirements.lock

RUN if [ "$ENV" = "dev" ] || [ "$ENV" = "test" ]; then \
    echo "Install all dependencies" \
    && RYE_TOOLCHAIN=/usr/bin/python rye sync --no-lock; \
    else \
    echo "Install production dependencies only" \
    && RYE_TOOLCHAIN=/usr/bin/python rye sync --no-dev --no-lock; \
    fi


ENTRYPOINT [ "/app/start.sh" ]