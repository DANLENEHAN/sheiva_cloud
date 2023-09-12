import boto3


def check_bucket_exists(s3_client: boto3.client, bucket_name: str):
    print(f"Checking S3 bucket: '{bucket_name}' exists")
    try:
        s3_client.list_objects_v2(Bucket=bucket_name)
    except Exception as exp:
        raise Exception(
            f"Critical error: unable to connect to S3 bucket {bucket_name} with exception: {exp.__repr__()}"
        )
    print(f"S3 bucket: '{bucket_name}' exists")
