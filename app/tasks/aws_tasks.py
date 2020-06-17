from datetime import datetime
from typing import Any, Dict, List

from ..utils.aws import get_cloudfront_client, get_s3_client


def expire_s3_objects(bucket, prefix) -> Dict[str, Any]:
    """
    Add new lifecycle rule to data lake bucket which will delete all
    objects with dataset/version/ prefix.
    Deletion might take up to 24 h.
    """
    rule = _expiration_rule(prefix)
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
    prefix: str, expiration_date=datetime.utcnow().date()
) -> Dict[str, Any]:
    """
    Define S3 lifecycle rule which will delete all files
    with prefix dataset/version/ within 24h
    """
    rule = {
        "Expiration": {"Date": expiration_date},
        "ID": f"delete_{prefix}".replace("/", "_"),
        "Filter": {"Prefix": f"{prefix}"},
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
    response = client.get_bucket_lifecycle_configuration(Bucket=bucket)
    return response["Rules"]
