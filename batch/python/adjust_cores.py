#!/usr/bin/env python

import os

import boto3

OOM_ERROR = "OutOfMemoryError: Container killed due to memory usage"


def print_new_cores_val():
    original_cores = int(os.getenv("CORES", os.cpu_count()))

    batch_client = boto3.client("batch", region_name=os.getenv("AWS_REGION"))

    job_id = os.getenv("AWS_BATCH_JOB_ID")
    jobs_desc = batch_client.describe_jobs(jobs=[job_id])

    new_cores = original_cores

    # For each previous attempt resulting in OOM, divide CORES by 2
    for attempt in jobs_desc["jobs"][0]["attempts"]:
        if attempt["container"].get("reason") == OOM_ERROR:
            new_cores = max(1, int(new_cores / 2))

    print(new_cores)


if __name__ == "__main__":
    exit(print_new_cores_val())
