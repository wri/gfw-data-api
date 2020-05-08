from typing import List, Optional, Dict
import os

from pydantic import BaseModel


class Job(BaseModel):
    job_name: str
    job_queue: str = os.environ.get("JOB_QUEUE", "")
    job_definition: str = os.environ.get("JOB_DEFINITION", "")
    command: List[str]
    environment: Dict[str, str] = {}
    vcpus: int = 1
    memory: int = 500
    attempts: int = 2
    attempt_duration_seconds: int = 3600
    parents: Optional[List[str]] = None
