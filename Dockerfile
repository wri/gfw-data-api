FROM tiangolo/uvicorn-gunicorn:python3.7-alpine3.8

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
COPY Pipfile Pipfile
COPY Pipfile.lock Pipfile.lock

RUN pipenv install --system --deploy --ignore-pipfile

RUN pip install pytest

# Remove build tools
RUN apk del libffi-dev g++ make

COPY ./app /app/app
COPY ./tests /app/tests

COPY alembic.ini /app/alembic.ini

COPY app/settings/prestart.sh /app/prestart.sh
