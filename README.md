# GFW Data API
High-performance Async REST API, in Python. FastAPI + GINO + Uvicorn (powered by PostgreSQL).

## Get Started
### Run Locally with Docker

1. Clone this Repository. `git clone https://github.com/wri/gfw-data-api.git`
2. Run `./scripts/setup` from the root directory. (Run `pip install pipenv` first, if necessary.)
3. Run locally using docker-compose. `./scripts/develop`

### Developing
* Generate a DB Migration: `./scripts/migrate` (note `app/settings/prestart.sh` will run migrations automatically when running `/scripts/develop`)
* Run tests: `./scripts/test`
* Run specific tests: `./scripts/test tasks/test_vector_source_assets.py::test_vector_source_asset`

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
