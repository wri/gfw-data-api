from app.tasks.aws_tasks import expire_s3_objects
from app.utils.aws import get_s3_client


def test_expire_s3_objects():

    bucket = "test_bucket"
    prefix = "prefix"
    tag = "tag"
    value = "value"

    # test environment uses moto server
    s3_client = get_s3_client()
    s3_client.create_bucket(Bucket=bucket)

    expire_s3_objects(bucket, prefix)
    response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
    assert len(response["Rules"]) == 1
    assert response["Rules"][0]["ID"] == f"delete_{prefix}_"
    assert response["Rules"][0]["Filter"] == {"Prefix": prefix}

    expire_s3_objects(bucket, tag, value)
    response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
    assert len(response["Rules"]) == 2
    assert response["Rules"][1]["ID"] == f"delete__{value}"
    assert response["Rules"][1]["Filter"] == {"Tag": {"Key": tag, "Value": value}}

    expire_s3_objects(bucket, prefix, tag, value)
    response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
    assert len(response["Rules"]) == 3
    assert response["Rules"][2]["ID"] == f"delete_{prefix}_{value}"
    assert response["Rules"][2]["Filter"] == {
        "Tag": {"And": {"Prefix": prefix, "Tags": [{"Key": tag, "Value": value}]}}
    }

    message = ""
    expire_s3_objects(bucket, prefix, tag)
    try:
        s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
    except ValueError as e:
        message = str(e)

    assert message == "Cannot create fitler using input data"
