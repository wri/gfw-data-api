import json
import os
import uuid
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from app.application import ContextEngine


class BatchJobMock:
    def __init__(self, job_desc: Sequence[Dict[str, Any]] = tuple()):
        self.jobs: List[uuid.UUID] = list()
        self.job_descriptions = job_desc

    def describe_jobs(self, *, jobs: List[str]) -> Dict[str, Any]:
        return {
            "jobs": [desc for desc in self.job_descriptions if desc["jobId"] in jobs]
        }

    def submit_batch_job(self, *args, **kwargs) -> uuid.UUID:
        job_id = uuid.uuid4()
        self.jobs.append(job_id)
        return job_id


async def get_user_mocked() -> Tuple[str, str]:
    return "userid_123", "USER"


async def get_admin_mocked() -> Tuple[str, str]:
    return "adminid_123", "ADMIN"


async def get_api_key_mocked() -> Tuple[Optional[str], Optional[str]]:
    return str(uuid.uuid4()), "localhost"


def void_function(*args, **kwargs) -> None:
    return


def bool_function_closure(value: bool, with_args=True) -> Callable:
    def bool_function(*args, **kwargs) -> bool:
        return value

    def simple_bool_function() -> bool:
        return value

    if with_args:
        return bool_function
    else:
        return simple_bool_function


def int_function_closure(value: int) -> Callable:
    def int_function(*args, **kwargs) -> int:
        return value

    return int_function


def dict_function_closure(value: Dict) -> Callable:
    def dict_function(*args, **kwargs) -> Dict:
        return value

    return dict_function


async def _create_vector_source_assets(dataset_name, version_name):
    # TODO: we currently only do the bare minimum here.
    #  still need to add gfw columns
    #  and check back in all task so that asset and versions are correctly set to saved
    from app.application import db

    with open(f"{os.path.dirname(__file__)}/fixtures/geojson/test.geojson") as src:
        geojson = json.load(src)

    async with ContextEngine("WRITE"):
        await db.all(
            f"""CREATE TABLE "{dataset_name}"."{version_name}" (fid integer, geom geometry);"""
        )
        await db.all(
            f"""INSERT INTO "{dataset_name}"."{version_name}" (fid, geom) SELECT 1,  ST_GeomFromGeoJSON('{json.dumps(geojson["features"][0]["geometry"])}');"""
        )
