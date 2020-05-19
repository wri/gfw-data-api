import boto3

from app.settings.globals import AWS_REGION

# S3_CLIENT = None
# BATCH_CLIENT = None


def client_constructor(service: str):
    """
    Using closure design for a client constructor
    This way we only need to create the client once in central location
    and it will be easier to mock
    """
    service_client = None

    def client():
        nonlocal service_client
        if service_client is None:
            service_client = boto3.client(service, region_name=AWS_REGION)
        return service_client

    return client


get_s3_client = client_constructor("s3")
get_batch_client = client_constructor("batch")

#
# def get_s3_client():
#     import boto3
#
#     global S3_CLIENT
#     if S3_CLIENT is None:
#         S3_CLIENT = boto3.client("s3", region_name=AWS_REGION)
#     return S3_CLIENT
#
#
# def get_batch_client():
#     import boto3
#
#     global BATCH_CLIENT
#     if BATCH_CLIENT is None:
#         BATCH_CLIENT = boto3.client("batch", region_name=AWS_REGION)
#     return BATCH_CLIENT
