import json
from pathlib import Path
from typing import Optional

import boto3
from starlette.config import Config
from starlette.datastructures import Secret

from ..models.pydantic.database import DatabaseURL

p: Path = Path(__file__).parents[2] / ".env"
config: Config = Config(p if p.exists() else None)

# TODO: Create seperat configs for read and write access with secrets coming from Secret Manager

client = boto3.client("secretsmanager")
response = client.get_secret_value(SecretId=config("SECRET_NAME", cast=str, default=None))
secrets = json.loads(response["SecretString"])

USERNAME = secrets["username"]
PASSWORD = secrets["password"]
DBNAME = secrets["dbname"]

DATABASE: str = config("DATABASE", cast=str)
DB_USER: Optional[str] = config("DB_USER", cast=str, default=None)
DB_PASSWORD: Optional[Secret] = config(
    "DB_PASSWORD", cast=Secret, default=None
)
DB_HOST: str = config("DB_HOST", cast=str, default="localhost")
DB_PORT: int = config("DB_PORT", cast=int, default=5432)


DATABASE_CONFIG: DatabaseURL = DatabaseURL(
    drivername="asyncpg",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DATABASE,
)
ALEMBIC_CONFIG: DatabaseURL = DatabaseURL(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DATABASE,
)
