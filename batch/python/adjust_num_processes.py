#!/usr/bin/env python

import os

import boto3

OOM_ERROR = "OutOfMemoryError: Container killed due to memory usage"


def calc_num_processes(job_id: str, original_num_proc, batch_client):
    jobs_desc = batch_client.describe_jobs(jobs=[job_id])

    new_num_proc = original_num_proc

    # For each previous attempt resulting in OOM, divide NUM_PROCESSES by 2
    for attempt in jobs_desc["jobs"][0]["attempts"]:
        if (
            attempt["container"].get("exitCode") == 137
            or attempt["container"].get("reason") == OOM_ERROR
        ):
            new_num_proc = max(1, int(new_num_proc / 2))

    return new_num_proc


if __name__ == "__main__":
    job_id = os.getenv("AWS_BATCH_JOB_ID")
    if job_id is None:
        raise ValueError("No AWS Batch Job ID found")
    original_num_proc = os.getenv("NUM_PROCESSES", os.getenv("CORES", os.cpu_count()))
    if original_num_proc is None:
        raise ValueError("Neither number of processes nor number of cores are set")
    else:
        original_num_proc = int(original_num_proc)

    batch_client = boto3.client("batch", region_name=os.getenv("AWS_REGION"))

    print(calc_num_processes(job_id, original_num_proc, batch_client))
