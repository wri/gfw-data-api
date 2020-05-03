FROM tiangolo/uvicorn-gunicorn:python3.7-alpine3.8

# Optional build argument for different environments
ARG ENV

RUN apk update && apk add gcc libffi-dev g++ postgresql-dev make

RUN pip install --upgrade pip && pip install pipenv

# Install rustup to get nightly build
RUN wget -O init.sh https://sh.rustup.rs
RUN sh init.sh -y
RUN cp $HOME/.cargo/bin/* /usr/local/bin

RUN rustup install nightly
RUN rustup default nightly

# use static linking to allow rust to compile orjson
ENV RUSTFLAGS "-C target-feature=-crt-static"

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

# Remove build tools
RUN apk del libffi-dev g++ make

COPY ./app /app/app

COPY alembic.ini /app/alembic.ini

COPY app/settings/prestart.sh /app/prestart.sh

# Set CMD depending on environment
CMD if [ "$ENV" = "test" ]; then \
	    pytest; \
    elif [ "$ENV" = "dev" ]; then \
	    /start-reload.sh; \
	else \
	    /start.sh; \
	fi
