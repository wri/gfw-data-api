FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8-slim

# Optional build argument for different environments
ARG ENV

RUN apt-get update -y \
    && apt-get install --no-install-recommends -y gcc libc-dev musl-dev postgresql-client libpq-dev

RUN pip install --upgrade pip && pip install pipenv

# Install python dependencies
# Install everything for dev and test otherwise just core dependencies
COPY Pipfile Pipfile
COPY Pipfile.lock Pipfile.lock

RUN if [ "$ENV" = "dev" ] || [ "$ENV" = "test" ]; then \
	     echo "Install all dependencies" && \
	     apt-get install -y --no-install-recommends git && \
	     pipenv install --system --deploy --ignore-pipfile --dev;  \
	else \
	     echo "Install production dependencies only" && \
	     pipenv install --system --deploy; \
	fi

RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY ./app /app/app

COPY alembic.ini /app/alembic.ini

COPY app/settings/prestart.sh /app/prestart.sh

COPY wait_for_postgres.sh /usr/local/bin/wait_for_postgres.sh
RUN chmod +x /usr/local/bin/wait_for_postgres.sh