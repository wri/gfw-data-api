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
  * `--no_build` - don't rebuild the containers
  * `--moto-port=<port_number>` - explicitly sets the motoserver port (default `50000`)
* Run specific tests: `./scripts/test tasks/test_vector_source_assets.py::test_vector_source_asset`
* Each development branch app instance gets its isolated database in AWS dev account that's cloned from `geostore` database. This database is named with the branch suffix (like `geostore_<branch_name>`). If a PR includes a database migration, once the change is merged to higher environments, the `geostore` database needs to also be updated with the migration. This can be done by manually replacing the existing database by a copy of a cleaned up version of the branch database (see `./prestart.sh` script for cloning command).
* Debug memory usage of Batch jobs with memory_profiler:
    1. Install memory_profiler in the job's Dockerfile
    2. Modify the job's script to run with memory_profiler. Ex: `pixetl "${ARG_ARRAY[@]}"` -> `mprof run -M -C -T 1 --python /usr/local/app/gfw_pixetl/pixetl.py "${ARG_ARRAY[@]}"`
    3. scp memory_profiler's .dat files off of the Batch instance (found in /tmp by default) while the instance is still up

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
