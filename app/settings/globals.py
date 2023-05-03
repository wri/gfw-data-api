import json
from pathlib import Path
from typing import Optional

from starlette.config import Config
from starlette.datastructures import Secret

from ..models.enum.pixetl import ResamplingMethod
from ..models.pydantic.database import DatabaseURL

#
# def _remove_revision(arn: str) -> str:
#     """Remove revision number from batch job description arn."""
#     arn_items = arn.split(":")
#     revision = arn_items.pop()
#     try:
#         # Check if revision is a number
#         int(revision)
#         return ":".join(arn_items)
#     except (ValueError, TypeError):
#         # if not, this means that there was no revision number in first place and we can use the input
#         return arn


# Read .env file, if exists
p: Path = Path(__file__).parents[2] / ".env"
config: Config = Config(p if p.exists() else None)

empty_db_secret = {
    "dbInstanceIdentifier": None,
    "dbname": None,
    "engine": None,
    "host": "localhost",
    "password": None,  # pragma: allowlist secret
    "port": 5432,
    "username": None,
}

empty_sa_secret = {"email": None, "token": None}

# As of writing, Fargate doesn't support to fetch secrets by key.
# Only entire secret object can be obtained.
DB_WRITER_SECRET = json.loads(
    config("DB_WRITER_SECRET", cast=str, default=json.dumps(empty_db_secret))
)
DB_READER_SECRET = json.loads(
    config("DB_READER_SECRET", cast=str, default=json.dumps(empty_db_secret))
)

SERVICE_ACCOUNT_SECRET = json.loads(
    config("SERVICE_ACCOUNT_SECRET", cast=str, default=json.dumps(empty_sa_secret))
)

ENV = config("ENV", cast=str, default="dev")

DATA_LAKE_BUCKET = config("DATA_LAKE_BUCKET", cast=str, default=None)

TILE_CACHE_BUCKET = config("TILE_CACHE_BUCKET", cast=str, default=None)
TILE_CACHE_CLOUDFRONT_ID = config("TILE_CACHE_CLOUDFRONT_ID", cast=str, default=None)
TILE_CACHE_URL = config("TILE_CACHE_URL", cast=str, default=None)
TILE_CACHE_CLUSTER = config("TILE_CACHE_CLUSTER", cast=str, default=None)
TILE_CACHE_SERVICE = config("TILE_CACHE_SERVICE", cast=str, default=None)

READER_USERNAME: Optional[str] = config(
    "DB_USER_RO", cast=str, default=DB_READER_SECRET["username"]
)
READER_PASSWORD: Optional[Secret] = config(
    "DB_PASSWORD_RO", cast=Secret, default=DB_READER_SECRET["password"]
)
READER_HOST: str = config("DB_HOST_RO", cast=str, default=DB_READER_SECRET["host"])
READER_PORT: int = config("DB_PORT_RO", cast=int, default=DB_READER_SECRET["port"])
READER_DBNAME = config("DATABASE_RO", cast=str, default=DB_READER_SECRET["dbname"])

WRITER_USERNAME: Optional[str] = config(
    "DB_USER", cast=str, default=DB_WRITER_SECRET["username"]
)
WRITER_PASSWORD: Optional[Secret] = config(
    "DB_PASSWORD", cast=Secret, default=DB_WRITER_SECRET["password"]
)
WRITER_HOST: str = config("DB_HOST", cast=str, default=DB_WRITER_SECRET["host"])
WRITER_PORT: int = config("DB_PORT", cast=int, default=DB_WRITER_SECRET["port"])
WRITER_DBNAME = config("DATABASE", cast=str, default=DB_WRITER_SECRET["dbname"])

if ENV == "dev":
    NAME_SUFFIX = config("NAME_SUFFIX", cast=str)
    READER_DBNAME = f"{READER_DBNAME}{NAME_SUFFIX}"
    WRITER_DBNAME = f"{WRITER_DBNAME}{NAME_SUFFIX}"

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

AWS_REGION = config("AWS_REGION", cast=str, default="us-east-1")

POSTGRESQL_CLIENT_JOB_DEFINITION = config("POSTGRESQL_CLIENT_JOB_DEFINITION", cast=str)
GDAL_PYTHON_JOB_DEFINITION = config("GDAL_PYTHON_JOB_DEFINITION", cast=str)
AURORA_JOB_QUEUE = config("AURORA_JOB_QUEUE", cast=str)
AURORA_JOB_QUEUE_FAST = config("AURORA_JOB_QUEUE_FAST", cast=str)
DATA_LAKE_JOB_QUEUE = config("DATA_LAKE_JOB_QUEUE", cast=str)
TILE_CACHE_JOB_DEFINITION = config("TILE_CACHE_JOB_DEFINITION", cast=str)
TILE_CACHE_JOB_QUEUE = config("TILE_CACHE_JOB_QUEUE", cast=str)
MAX_CORES = config("MAX_CORES", cast=int, default=96)
MAX_MEM = config("MAX_MEM", cast=int, default=760000)
PIXETL_JOB_DEFINITION = config("PIXETL_JOB_DEFINITION", cast=str)
PIXETL_JOB_QUEUE = config("PIXETL_JOB_QUEUE", cast=str)
PIXETL_CORES = config("PIXETL_CORES", cast=int, default=48)
PIXETL_MAX_MEM = config("PIXETL_MAX_MEM", cast=int, default=380000)
PIXETL_DEFAULT_RESAMPLING = config(
    "DEFAULT_RESAMPLING", cast=str, default=ResamplingMethod.nearest.value
)
RASTER_ANALYSIS_LAMBDA_NAME = config("RASTER_ANALYSIS_LAMBDA_NAME", cast=str)


POLL_WAIT_TIME = config("POLL_WAIT_TIME", cast=int, default=30)
CHUNK_SIZE = config("CHUNK_SIZE", cast=int, default=50)
API_URL = config("API_URL", cast=str)

SERVICE_ACCOUNT_TOKEN = config(
    "SERVICE_ACCOUNT_TOKEN", cast=str, default=SERVICE_ACCOUNT_SECRET["token"]
)

LAMBDA_ENTRYPOINT_URL = config(
    "LAMBDA_ENTRYPOINT_URL",
    cast=str,
    default=f"https://lambda.{AWS_REGION}.amazonaws.com",
)

S3_ENTRYPOINT_URL = config("S3_ENTRYPOINT_URL", cast=str, default=None)
SQL_REQUEST_TIMEOUT = 58

AWS_GCS_KEY_SECRET_ARN = config("AWS_GCS_KEY_SECRET_ARN", cast=str, default=None)
AWS_SECRETSMANAGER_URL = config("AWS_SECRETSMANAGER_URL", cast=str, default=None)

RW_API_URL = config("RW_API_URL", cast=str, default=None)

HOUR: int = int(60 * 60)
# Temporarily set high timeout for TCLF. See https://gfw.atlassian.net/browse/GTC-1843
# DEFAULT_JOB_DURATION: int = int(HOUR * 2)
DEFAULT_JOB_DURATION: int = 400000

API_KEY_NAME = config("API_KEY_NAME", cast=str, default="x-api-key")
GEOSTORE_SIZE_LIMIT_OTF = config(
    "GEOSTORE_SIZE_LIMIT_OTF", cast=int, default=1000000000
)

API_GATEWAY_ID = config("API_GATEWAY_ID", cast=str)
API_GATEWAY_INTERNAL_USAGE_PLAN = config("API_GATEWAY_INTERNAL_USAGE_PLAN", cast=str)
API_GATEWAY_EXTERNAL_USAGE_PLAN = config("API_GATEWAY_EXTERNAL_USAGE_PLAN", cast=str)
API_GATEWAY_STAGE_NAME = config("API_GATEWAY_STAGE_NAME", cast=str)

default_domains = ",".join(
    [
        "*.globalforestwatch.org",
        "globalforestwatch.org",
        "api.resourcewatch.org",
        "my.gfw-mapbuilder.org",
        "resourcewatch.org",
    ]
)

INTERNAL_DOMAINS = config("INTERNAL_DOMAINS", cast=str, default=default_domains)
GOOGLE_APPLICATION_CREDENTIALS = config(
    "GOOGLE_APPLICATION_CREDENTIALS", cast=str, default="/root/.gcs/private_key.json"
)
