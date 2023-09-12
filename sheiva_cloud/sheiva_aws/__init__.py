import os

import boto3


def get_boto3_session() -> boto3.Session:
    """
    Provides ways for us to locally and in-cloud create boto3 sessions. FYI
    AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID are reserved for AWS so don't
    use them as environment variables.
    Returns a boto3 session. If LOCAL_AWS_ACCESS_KEY_ID and LOCAL_AWS_SECRET_ACCESS_KEY
    are set as environment variables, then the session will be created with
    those credentials. Otherwise, the session will be created with the
    credentials of the IAM role that the lambda function is running under.

    Returns:
            boto3.Session: boto3 session
    """

    LOCAL_AWS_ACCESS_KEY_ID = os.getenv("LOCAL_AWS_ACCESS_KEY_ID", "")
    LOCAL_AWS_SECRET_ACCESS_KEY = os.getenv("LOCAL_AWS_SECRET_ACCESS_KEY", "")
    REGION = os.getenv("REGION", "eu-west-1")

    if LOCAL_AWS_ACCESS_KEY_ID and LOCAL_AWS_SECRET_ACCESS_KEY:
        return boto3.Session(
            aws_access_key_id=LOCAL_AWS_ACCESS_KEY_ID,
            aws_secret_access_key=LOCAL_AWS_SECRET_ACCESS_KEY,
            region_name=REGION,
        )

    return boto3.Session()
