#!/usr/bin/env python

# Required options:
# --dataset {dataset}
# --implementation {implementation}
#
# Assumes that the cog file to be uploaded is available at {implementation}.tif
# Uploads the cog file to GCS at {GCS_BUCKET}/{dataset}/{implementation}.tif and
# creates a GEE asset that links to that GCS URI.

import json
import os

import boto3
import ee
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
from typer import Option, run

EE_PROJECT = "forma-250"
GCS_BUCKET = "data-api-gee-assets"
GCS_SECRET_KEY_ARN = os.environ["AWS_GCS_KEY_SECRET_ARN"]
GCS_CREDENTIALS_FILE = "gcs_credentials.json"
AWS_REGION = os.environ["AWS_REGION"]


def set_google_application_credentials():
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    response = client.get_secret_value(SecretId=GCS_SECRET_KEY_ARN)

    with open(GCS_CREDENTIALS_FILE, "w") as f:
        f.write(response["SecretString"])

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_CREDENTIALS_FILE

    return json.loads(response["SecretString"])["client_email"]


def upload_cog_to_gcs(dataset, implementation):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"{dataset}/{implementation}.tif")

    blob.upload_from_filename(f"{implementation}.tif")

    return f"gs://{GCS_BUCKET}/{dataset}/{implementation}.tif"


def create_cog_backed_asset(dataset, implementation, gcs_path, credentials):
    # delete any existing asset with the same dataset/implementation
    try:
        ee.data.deleteAsset(f"projects/{EE_PROJECT}/assets/{dataset}/{implementation}")
    except ee.EEException:
        # asset doesn't exist
        pass

    # create dataset folder if it doesn't exist
    try:
        ee.data.createAsset(
            {"type": "Folder"}, f"projects/{EE_PROJECT}/assets/{dataset}"
        )
    except ee.EEException:
        # folder already exists
        pass

    # link GCS COG to the GEE asset
    session = AuthorizedSession(credentials.with_quota_project(EE_PROJECT))
    request = {"type": "IMAGE", "gcs_location": {"uris": [gcs_path]}}

    asset_id = f"{dataset}/{implementation}"
    url = "https://earthengine.googleapis.com/v1alpha/projects/{}/assets?assetId={}"

    response = session.post(
        url=url.format(EE_PROJECT, asset_id), data=json.dumps(request)
    )

    if response.status_code != 200:
        raise Exception(
            f"GEE returned unexpected status code {response.status_code} with payload {response.content}"
        )

    return asset_id


def ingest_in_gee(dataset, implementation, gcs_path):
    """Ingest directly into GEE as a best effort task."""
    # delete any existing asset with the same dataset/implementation
    try:
        ee.data.deleteAsset(f"projects/{EE_PROJECT}/assets/{dataset}/{implementation}")
    except ee.EEException:
        # asset doesn't exist
        pass

    # create dataset folder if it doesn't exist
    try:
        ee.data.createAsset(
            {"type": "Folder"}, f"projects/{EE_PROJECT}/assets/{dataset}"
        )
    except ee.EEException:
        # folder already exists
        pass

    asset_id = f"{dataset}/{implementation}"
    request_id = ee.data.newTaskId()[0]
    params = {
        "name": f"projects/{EE_PROJECT}/assets/{asset_id}",
        "tilesets": [{"sources": [{"uris": [gcs_path]}]}],
    }
    ee.data.startIngestion(request_id=request_id, params=params)
    return asset_id


def set_acl_to_anyone_read(asset_id):
    # update ACL to be public
    full_asset_id = f"projects/{EE_PROJECT}/assets/{asset_id}"
    acl = ee.data.getAssetAcl(full_asset_id)
    acl["all_users_can_read"] = True
    ee.data.setAssetAcl(full_asset_id, acl)


def export_to_gee(
    dataset: str = Option(..., help="Dataset name."),
    implementation: str = Option(..., help="Implementation name."),
):
    service_account = set_google_application_credentials()

    # initialize GEE
    credentials = ee.ServiceAccountCredentials(service_account, GCS_CREDENTIALS_FILE)
    ee.Initialize(credentials)

    gcs_path = upload_cog_to_gcs(dataset, implementation)
    ingest_in_gee(dataset, implementation, gcs_path)


if __name__ == "__main__":
    run(export_to_gee)
