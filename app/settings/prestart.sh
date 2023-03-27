#!/usr/bin/env bash
if [ "${ENV}" = "dev" ]; then
    # in dev environment, we clone a db instance for the branch from a template database

    # parse out DB credentials from the secret json object
    DB_HOST=$(jq -nr 'env.DB_WRITER_SECRET' | jq '.host' | sed 's/"//g')
    DB_PORT=$(jq -nr 'env.DB_WRITER_SECRET' | jq '.port' | sed 's/"//g')
    DB_USER=$(jq -nr 'env.DB_WRITER_SECRET' | jq '.username' | sed 's/"//g')
    DB_PASSWORD=$(jq -nr 'env.DB_WRITER_SECRET' | jq '.password' | sed 's/"//g')
    DATABASE_MAIN=$(jq -nr 'env.DB_WRITER_SECRET' | jq '.dbname' | sed 's/"//g') # template database
    DATABASE="$DATABASE_MAIN$NAME_SUFFIX" # branch database

    # return the branch database if it exists in pg_database. if not, create it.
    PGPASSWORD=$DB_PASSWORD psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DATABASE_MAIN} \
        -tc "SELECT 1 FROM pg_database WHERE datname = '$DATABASE'" | grep -q 1 \
    || PGPASSWORD=$DB_PASSWORD psql -h ${DB_HOST} \
        -p ${DB_PORT} -U ${DB_USER} -d ${DATABASE_MAIN} \
        -c "CREATE DATABASE $DATABASE WITH TEMPLATE ${DATABASE_MAIN}_template OWNER $DB_USER"
fi

alembic upgrade head