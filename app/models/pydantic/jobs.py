from typing import List, Optional

from pydantic import BaseModel


class Job(BaseModel):
    job_name: str
    job_queue: str
    job_definition: str
    command: List[str]
    vcpus: int = 1
    memory: int = 500
    attempts: int = 2
    attempt_duration_seconds: int = 3600
    parents: Optional[List[str]] = None
