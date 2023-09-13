import boto3


def check_bucket_exists(s3_client: boto3.client, bucket_name: str):
    """
    Checks if an s3 bucket exists.
    """

    print(f"Checking S3 bucket: '{bucket_name}' exists")
    try:
        s3_client.list_objects_v2(Bucket=bucket_name)
    except Exception as exp:
        # pylint: disable=broad-exception-raised,raise-missing-from
        raise Exception(
            "Critical error: unable to connect to S3 bucket "
            f"{bucket_name} with exception: {repr(exp)}"
        )
    print(f"S3 bucket: '{bucket_name}' exists")
