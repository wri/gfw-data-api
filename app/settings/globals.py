import json
from pathlib import Path
from typing import Optional, Dict, Any

import boto3
from starlette.config import Config
from starlette.datastructures import Secret

from ..models.pydantic.database import DatabaseURL

p: Path = Path(__file__).parents[2] / ".env"
config: Config = Config(p if p.exists() else None)
client = boto3.client("secretsmanager")


def get_secret(name: str) -> Dict[str, Any]:
    secret = client.get_secret_value(SecretId=name)
    return json.loads(secret["SecretString"])


ENV = config("ENV", cast=str, default="dev")
BUCKET = config("BUCKET", cast=str, default=None)

if ENV == "docker" or ENV == "test":

    READER_USERNAME: Optional[str] = config("DB_USER", cast=str, default=None)
    READER_PASSWORD: Optional[Secret] = config("DB_PASSWORD", cast=Secret, default=None)
    READER_HOST: str = config("DB_HOST", cast=str, default="localhost")
    READER_PORT: int = config("DB_PORT", cast=int, default=5432)
    if ENV == "test":
        READER_DBNAME: str = config("TEST_DATABASE", cast=str)
    else:
        READER_DBNAME = config("DATABASE", cast=str)

    WRITER_USERNAME = READER_USERNAME
    WRITER_PASSWORD = READER_PASSWORD
    WRITER_DBNAME = READER_DBNAME
    WRITER_HOST = READER_HOST
    WRITER_PORT = READER_PORT


else:
    READER_SECRET_NAME = config("READER_SECRET_NAME", cast=str, default=None)
    WRITER_SECRET_NAME = config("WRITER_SECRET_NAME", cast=str, default=None)

    secrets = get_secret(READER_SECRET_NAME)
    READER_USERNAME = secrets["username"]
    READER_PASSWORD = secrets["password"]
    READER_DBNAME = secrets["dbname"]
    READER_HOST = secrets["host"]
    READER_PORT = secrets["port"]

    secrets = get_secret(WRITER_SECRET_NAME)
    WRITER_USERNAME = secrets["username"]
    WRITER_PASSWORD = secrets["password"]
    WRITER_DBNAME = secrets["dbname"]
    WRITER_HOST = secrets["host"]
    WRITER_PORT = secrets["port"]


DATABASE_CONFIG: DatabaseURL = DatabaseURL(
    drivername="asyncpg",
    username=READER_USERNAME,
    password=READER_PASSWORD,
    host=READER_HOST,
    port=READER_PORT,
    database=READER_DBNAME,
)

WRITE_DATABASE_CONFIG: DatabaseURL = DatabaseURL(
    drivername="asyncpg",
    username=WRITER_USERNAME,
    password=WRITER_PASSWORD,
    host=WRITER_HOST,
    port=WRITER_PORT,
    database=WRITER_DBNAME,
)

ALEMBIC_CONFIG: DatabaseURL = DatabaseURL(
    drivername="postgresql+psycopg2",
    username=WRITER_USERNAME,
    password=WRITER_PASSWORD,
    host=WRITER_HOST,
    port=WRITER_PORT,
    database=WRITER_DBNAME,
)
