"""
Lambda function that sends messages to the workout file
transform queue.
"""

import os
from typing import List
from random import sample

import boto3

from sheiva_cloud.sheiva_aws.sqs import WORKOUT_FILE_TRANSFORM_QUEUE
from sheiva_cloud.sheiva_aws.sqs.clients import StandardClient
from sheiva_cloud.sheiva_aws.s3 import SHEIVA_SCRAPE_BUCKET

TRANSFORM_LIMIT = os.getenv("TRANSFORM_LIMIT", None)


def get_scraped_file_paths(s3_client: boto3.client) -> List:
    """
    Gets a list of bucket keys for scraped Highrise Workout
    Data.
    Returns:
        List: list of bucket keys for scraped workout files.
    """

    print("Getting scraped workout files")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=SHEIVA_SCRAPE_BUCKET)
    return [
        f["Key"]
        for f in page_iterator.search(
            "Contents[?starts_with(Key, 'highrise/workout-data') "
            "&& ends_with(Key, '.json')]"
        )
    ]


def get_transformed_file_paths(s3_client: boto3.client) -> List[str]:
    """
    Gets a list of bucket keys for transformed Highrise Workout
    Data.
    Returns:
        List[str]: list of bucket keys for transformed workout files.
    """

    print("Getting transformed workout files")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=SHEIVA_SCRAPE_BUCKET)
    return [
        f["Key"]
        for f in page_iterator.search(
            "Contents[?starts_with(Key, "
            "'highrise/workout-data/transformed/workouts') "
            "&& ends_with(Key, '.csv')]"
        )
    ]


def get_transform_canidates(s3_client: boto3.client) -> List[str]:
    """
    Retrives scraped files and transformed files. A transformed
    file has the same uuid file name as it's source file. Gets
    a list of all the scraped files that has not been transformed.
    Returns:
        List[str]: list of all scraped files to be transformed.
    """

    scraped_files = get_scraped_file_paths(s3_client=s3_client)
    print(f"Retreived {len(scraped_files)} scraped file bucket keys")
    transformed_files_uuids = [
        key.split(".")[0]
        for key in get_transformed_file_paths(s3_client=s3_client)
    ]
    return [
        key
        for key in scraped_files
        if key.split(".")[0] not in transformed_files_uuids
    ]


def send_messages_to_transform_queue(
    sqs_client: boto3.client, files_to_transform: List[str]
):
    transform_queue = StandardClient(
        queue_url=WORKOUT_FILE_TRANSFORM_QUEUE, sqs_client=sqs_client
    )
    for message in files_to_transform:
        print(message)
        transform_queue.send_message(
            message_body="Empty Body",
            message_attributes={
                "s3_input_file": {
                    "DataType": "String",
                    "StringValue": message,
                },
                "s3_output_bucket_key": {
                    "DataType": "String",
                    "StringValue": f"highrise/transformed/workout-data",
                },
            },
        )


# pylint: disable=unused-argument
def handler(event, context):
    """
    Lambda handler for scraping workout links.
    Args:
        event (Dict): event object
        context (Dict): context object
    """

    boto3_session = boto3.Session()
    s3_client = boto3_session.client("s3")
    sqs_client = boto3_session.client("sqs")

    if TRANSFORM_LIMIT is None:
        raise ValueError("TRANSFORM_LIMIT env variable not set")

    files_to_transform = sample(
        get_transform_canidates(s3_client=s3_client), TRANSFORM_LIMIT
    )

    print(f"Sending {len(files_to_transform)} messages to transform queue")

    send_messages_to_transform_queue(
        sqs_client=sqs_client, files_to_transform=files_to_transform
    )

    print("Done")


if __name__ == "__main__":
    handler(event=None, context=None)