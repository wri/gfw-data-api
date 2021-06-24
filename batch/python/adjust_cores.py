#!/usr/bin/env python

import os
from uuid import UUID

import boto3

OOM_ERROR = "OutOfMemoryError: Container killed due to memory usage"


def calc_new_cores_val(job_id: UUID, original_cores, batch_client):
    jobs_desc = batch_client.describe_jobs(jobs=[job_id])

    new_cores = original_cores

    # For each previous attempt resulting in OOM, divide CORES by 2
    for attempt in jobs_desc["jobs"][0]["attempts"]:
        if attempt["container"].get("reason") == OOM_ERROR:
            new_cores = max(1, int(new_cores / 2))

    return new_cores


if __name__ == "__main__":
    job_id = UUID(os.getenv("AWS_BATCH_JOB_ID"))
    original_cores = os.getenv("CORES", os.cpu_count())
    if original_cores is None:
        raise ValueError("This makes mypy happy")
    else:
        original_cores = int(original_cores)

    batch_client = boto3.client("batch", region_name=os.getenv("AWS_REGION"))

    print(calc_new_cores_val(job_id, original_cores, batch_client))
