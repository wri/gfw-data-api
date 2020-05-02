from pathlib import Path
from typing import Optional

from starlette.config import Config
from starlette.datastructures import Secret

from ..models.pydantic.database import DatabaseURL

# Read .env file, if exists
p: Path = Path(__file__).parents[2] / ".env"
config: Config = Config(p if p.exists() else None)

ENV = config("ENV", cast=str, default="dev")
BUCKET = config("BUCKET", cast=str, default=None)

READER_USERNAME: Optional[str] = config("DB_USER_RO", cast=str, default=None)
READER_PASSWORD: Optional[Secret] = config("DB_PASSWORD_RO", cast=Secret, default=None)
READER_HOST: str = config("DB_HOST_RO", cast=str, default="localhost")
READER_PORT: int = config("DB_PORT_RO", cast=int, default=5432)
READER_DBNAME = config("DATABASE_RO", cast=str)

WRITER_USERNAME: Optional[str] = config("DB_USER", cast=str, default=None)
WRITER_PASSWORD: Optional[Secret] = config("DB_PASSWORD", cast=Secret, default=None)
WRITER_HOST: str = config("DB_HOST", cast=str, default="localhost")
WRITER_PORT: int = config("DB_PORT", cast=int, default=5432)
WRITER_DBNAME = config("DATABASE", cast=str)


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
