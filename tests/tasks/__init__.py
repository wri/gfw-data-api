from time import sleep
from typing import List, Set

import requests

from app.utils.aws import get_batch_client


async def poll_jobs(job_ids: List[str]) -> str:
    client = get_batch_client()
    failed_jobs: Set[str] = set()
    completed_jobs: Set[str] = set()
    pending_jobs: Set[str] = set(job_ids)

    while True:
        response = client.describe_jobs(
            jobs=list(pending_jobs.difference(completed_jobs))
        )

        for job in response["jobs"]:
            print(
                f"Container for job {job['jobId']} exited with status {job['status']}"
            )
            if job["status"] == "SUCCEEDED":
                print(f"Container for job {job['jobId']} succeeded")

                completed_jobs.add(job["jobId"])
            if job["status"] == "FAILED":
                print(f"Container for job {job['jobId']} failed")

                failed_jobs.add(job["jobId"])

        if completed_jobs == set(job_ids):

            return "saved"
        elif failed_jobs:

            return "failed"

        sleep(1)


def check_callbacks(task_ids, port):
    get_resp = requests.get(f"http://localhost:{port}")
    req_list = get_resp.json()["requests"]

    assert len(req_list) == len(task_ids)

    task_path = [f"/tasks/{taskid}" for taskid in task_ids]
    req_paths = list()
    for i, req in enumerate(req_list):
        req_paths.append(req["path"])
        assert req["body"]["change_log"][0]["status"] == "success"

    # Order of task might vary if running in parallel.
    # We only check if URLs were correct
    assert all(elem in task_path for elem in req_paths)
