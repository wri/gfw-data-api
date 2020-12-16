# GFW Data API
High-performance Async REST API, in Python. FastAPI + GINO + Uvicorn (powered by PostgreSQL).

## Get Started
### Run Locally

1. Clone this Repository. `git clone https://github.com/wri/gfw-data-api.git`
2. Run `pipenv install --dev` from root. (Run `pip install pipenv` first, if necessary.)
3. Make a copy of `.dist.env`, rename to `.env`. Fill in PostgreSQL connection vars.
4. Generate DB Migrations: `alembic revision --autogenerate`. It will be applied when the application starts. You can trigger manually with `alembic upgrade head`.
5. Run:
    - FastAPI Application:
        * _For Active Development (w/ auto-reload):_ Run locally with `pipenv run uvicorn app.main:app --reload `
        * _For Debugging (compatible w/ debuggers, no auto-reload):_ Configure debugger to run `python app/main.py`.

### Run Locally with Docker
1. Clone this Repository. `git clone https://github.com/leosussan/fastapi-gino-arq-uvicorn.git`
2. Generate a DB Migration: `alembic revision --autogenerate`.*
3. Run locally using docker-compose. `docker-compose -f docker-compose.local.yml -f docker-compose.worker.yml -f docker-compose.yml up --build`.

*`app/settings/prestart.sh` will run migrations for you before the app starts.

## Features
### Core Dependencies
* **FastAPI:** touts performance on-par with NodeJS & Go + automatic Swagger + ReDoc generation.
* **GINO:** built on SQLAlchemy core. Lightweight, simple, asynchronous ORM for PostgreSQL.
* **Uvicorn:** Lightning-fast, asynchronous ASGI server.
* **Optimized Dockerfile:** Optimized Dockerfile for ASGI applications, from https://github.com/tiangolo/uvicorn-gunicorn-docker.

#### Additional Dependencies
* **Pydantic:** Core to FastAPI. Define how data should be in pure, canonical python; validate it with pydantic.
* **Alembic:** Handles database migrations. Compatible with GINO.
* **SQLAlchemy_Utils:** Provides essential handles & datatypes. Compatible with GINO.
* **PostgreSQL:** Robust, fully-featured, scalable, open-source.
