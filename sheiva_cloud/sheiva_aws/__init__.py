import os

import boto3


def get_boto3_session() -> boto3.Session:
    """
    Returns a boto3 session. If AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    are set as environment variables, then the session will be created with
    those credentials. Otherwise, the session will be created with the
    credentials of the IAM role that the lambda function is running under.

    Returns:
            boto3.Session: boto3 session
    """

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        return boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )

    return boto3.Session()
