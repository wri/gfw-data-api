from datetime import datetime
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from ..utils.aws import get_cloudfront_client, get_s3_client


def delete_s3_objects(bucket: str, prefix: str,) -> int:
    """
    S3 list_objects_v2 and delete_objects paginate responses in chunks of 1000
    We need to use parginator object to retrieve objects and then delete 1000 at a time
    https://stackoverflow.com/a/43436769/1410317
    """

    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    delete_us: Dict[str, List[Dict[str, str]]] = {"Objects": []}
    count = 0

    for item in pages.search("Contents"):
        if item:
            delete_us["Objects"].append({"Key": item["Key"]})

            # flush once aws limit reached
            if len(delete_us["Objects"]) >= 1000:
                count += len(delete_us["Objects"])
                client.delete_objects(Bucket=bucket, Delete=delete_us)
                delete_us = dict(Objects=[])

    # flush rest
    if len(delete_us["Objects"]):
        count += len(delete_us["Objects"])
        client.delete_objects(Bucket=bucket, Delete=delete_us)

    return count


def expire_s3_objects(
    bucket: str,
    prefix: Optional[str] = None,
    key: Optional[str] = None,
    value: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Add new lifecycle rule to data lake bucket which will delete all
    objects with dataset/version/ prefix.
    Deletion might take up to 24 h.
    """
    rule = _expiration_rule(prefix, key, value)
    return _update_lifecycle_rule(bucket, rule)


def flush_cloudfront_cache(cloudfront_id: str, path: str) -> Dict[str, Any]:
    """
    Flush tile cache cloudfront cache for a given path
    """
    client = get_cloudfront_client()

    response = client.create_invalidation(
        DistributionId=cloudfront_id,
        InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": [path]},
            "CallerReference": str(datetime.timestamp(datetime.now())).replace(".", ""),
        },
    )
    return response


def _expiration_rule(
    prefix: Optional[str] = None,
    key: Optional[str] = None,
    value: Optional[str] = None,
    expiration_date=datetime.utcnow(),
) -> Dict[str, Any]:
    """
    Define S3 lifecycle rule which will delete all files
    with prefix dataset/version/ within 24h
    """

    if prefix and key and value:
        filter: Dict[str, Any] = {
            "And": {"Prefix": prefix, "Tags": [{"Key": key, "Value": value}]}
        }
    elif prefix and not key and not value:

        filter = {"Prefix": prefix}
    elif not prefix and key and value:
        filter = {"Tags": {"Key": key, "Value": value}}
    else:
        raise ValueError("Cannot create filter using input data")

    rule = {
        "Expiration": {"Date": expiration_date},
        "ID": f"delete_{prefix}_{value}".replace("/", "_").replace(".", "_"),
        "Filter": filter,
        "Status": "Enabled",
    }
    return rule


def _update_lifecycle_rule(bucket, rule) -> Dict[str, Any]:
    """
    Add new lifecycle rule to bucket
    """
    client = get_s3_client()
    rules = _get_lifecycle_rules(bucket)
    rules.append(rule)
    response = client.put_bucket_lifecycle_configuration(
        Bucket="string", LifecycleConfiguration={"Rules": rules}
    )
    return response


def _get_lifecycle_rules(bucket: str) -> List[Dict[str, Any]]:
    """
    Get current lifecycle rules for bucket
    """
    client = get_s3_client()

    try:
        response = client.get_bucket_lifecycle_configuration(Bucket=bucket)
    except ClientError as e:
        if "NoSuchLifecycleConfiguration" in str(e):
            rules = []
        else:
            raise
    else:
        rules = response["Rules"]

    return rules
