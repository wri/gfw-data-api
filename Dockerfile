FROM tiangolo/uvicorn-gunicorn-fastapi:python3.10-slim

# Optional build argument for different environments
ARG ENV

RUN apt-get update -y \
    && apt-get install --no-install-recommends -y gcc g++ libc-dev \
        postgresql-client libpq-dev make git jq libgdal-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && pip install pipenv==2024.0.1
#TODO move to pipfile when operational
RUN pip install newrelic

# Install python dependencies
# Install everything for dev and test otherwise just core dependencies
COPY Pipfile Pipfile
COPY Pipfile.lock Pipfile.lock

RUN if [ "$ENV" = "dev" ] || [ "$ENV" = "test" ]; then \
    echo "Install all dependencies" \
    && pipenv install --system --deploy --ignore-pipfile --dev; \
    else \
    echo "Install production dependencies only" \
    && pipenv install --system --deploy; \
    fi

COPY ./app /app/app

COPY alembic.ini /app/alembic.ini

COPY app/settings/prestart.sh /app/prestart.sh
COPY app/settings/start.sh /app/start.sh
COPY newrelic.ini /app/newrelic.ini

COPY wait_for_postgres.sh /usr/local/bin/wait_for_postgres.sh
RUN chmod +x /usr/local/bin/wait_for_postgres.sh
RUN chmod +x /app/start.sh

ENTRYPOINT [ "/app/start.sh" ]