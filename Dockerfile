FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8-slim

# Optional build argument for different environments
ARG ENV

RUN apt-get update -y && apt-get install -y postgresql-client

RUN pip install --upgrade pip && pip install pipenv

# Install python dependencies
# Install everything for dev and test otherwise just core dependencies
COPY Pipfile Pipfile
COPY Pipfile.lock Pipfile.lock

RUN if [ "$ENV" = "dev" ] || [ "$ENV" = "test" ]; then \
	     echo "Install all dependencies" && \
	     pipenv install --system --deploy --ignore-pipfile --dev;  \
	else \
	     echo "Install production dependencies only" && \
	     pipenv install --system --deploy; \
	fi

COPY ./app /app/app

COPY alembic.ini /app/alembic.ini

COPY app/settings/prestart.sh /app/prestart.sh

COPY wait_for_postgres.sh /usr/local/bin/wait_for_postgres.sh
RUN chmod +x /usr/local/bin/wait_for_postgres.sh

# Set CMD depending on environment
CMD if [ "$ENV" = "test" ]; then \
	    wait_for_postgres.sh pytest; \
    elif [ "$ENV" = "dev" ]; then \
	    /start-reload.sh; \
	else \
	    /start.sh; \
	fi
