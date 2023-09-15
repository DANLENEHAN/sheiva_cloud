"""
Lambda function for putting workout links into the workout link queue.
Requires the following environment variables:
    - WORKOUTLINK_QUEUE_URL
    - WORKOUT_SCRAPE_TRIGGER_QUEUE_URL: url of the workout link SQS queue
    - WORKOUT_LINKS_BUCKET: name of the s3 bucket
"""

import json
import os
from typing import Dict, List, Tuple

import boto3

from sheiva_cloud.sheiva_aws.s3.functions import check_bucket_exists
from sheiva_cloud.sheiva_aws.sqs.functions import parse_sqs_message_data
from sheiva_cloud.sheiva_aws.sqs.standard_sqs import StandardSQS

WORKOUTLINK_QUEUE_URL = os.getenv("WORKOUTLINK_QUEUE_URL", "")
SHEIVA_SCRAPE_BUCKET = os.getenv("SHEIVA_SCRAPE_BUCKET", "")
WORKOUT_SCRAPE_TRIGGER_QUEUE_URL = os.getenv(
    "WORKOUT_SCRAPE_TRIGGER_QUEUE_URL", ""
)
GENDER = os.getenv("GENDER", "")


def parse_workout_scrape_trigger_message(message: Dict) -> Tuple[int, str]:
    """
    Parses the workout scrape trigger message.
    Args:
        message (Dict): message from the workout scrape trigger queue
    Returns:
        int: number of workout links to scrape
    """

    print("Parsing workout scrape trigger message")
    try:
        return int(message["body"]), message["receiptHandle"]
    # pylint: disable=broad-except
    except Exception as e:
        print(f"Error parsing workout scrape trigger message: {repr(e)}")
        return 0, ""


def get_workout_link_bucket_dirs(s3_client: boto3.client) -> List:
    """
    Builds a list of workout link bucket dirs.
    Returns:
        List: list of workout link buckets dirs.
    """

    print("Getting workout link bucket dirs")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=SHEIVA_SCRAPE_BUCKET)
    return [
        f["Key"]
        for f in page_iterator.search(
            "Contents[?starts_with(Key, 'user-data/user-workout-links"
            f"/{GENDER}/') && ends_with(Key, '.json')]"
        )
    ]


def send_workout_links_to_queue(
    workout_links: List,
    bucket_key: str,
    workout_link_queue: boto3.client,
) -> str:
    """
    Sends workout links to the workout link queue.
    Args:
        workout_links (List): list of workout links
        bucket_key (str): key of the s3 bucket
        workout_link_queue (boto3.client): workout link queue
    """

    print(
        f"Sending {len(workout_links)} workout links "
        f"bucket_key: '{bucket_key}' to workout link queue"
    )
    workout_link_queue.send_message(
        message_body=json.dumps(workout_links),
        message_attributes={
            "bucket_key": {
                "StringValue": bucket_key,
                "DataType": "String",
            }
        },
    )

    return "Success"


def get_and_post_workout_links(
    s3_client: boto3.client,
    workout_link_queue: boto3.resource,
    workout_link_bucket_dirs: List,
    num_workout_links_to_scrape: int,
) -> str:
    """
    Gets and posts workout links to the workout link queue.
    Args:
        s3_client (boto3.client): s3 client
        workout_link_queue (boto3.resource): workout link queue
        workout_link_bucket_dirs (List): list of workout link bucket dirs
        num_workout_links_to_scrape (int): number of workout links
            to scrape per age group
    """

    for bucket_dir in workout_link_bucket_dirs:
        print(f"Getting workout links from bucket: {bucket_dir}")
        bucket = s3_client.get_object(
            Bucket=SHEIVA_SCRAPE_BUCKET, Key=bucket_dir
        )
        bucket_contents = json.loads(bucket["Body"].read().decode("utf-8"))
        workout_links = bucket_contents[:num_workout_links_to_scrape]
        print(f"Sending {len(workout_links)} workout links to queue")
        age_group = bucket_dir.split("/")[-1].split(".")[0]
        send_workout_links_to_queue(
            workout_links=workout_links,
            bucket_key=f"workout-data/{GENDER}/{age_group}",
            workout_link_queue=workout_link_queue,
        )
        print(
            f"Deleting {len(workout_links)} workout "
            f"links from bucket {bucket_dir}"
        )
        bucket_contents = bucket_contents[num_workout_links_to_scrape:]
        s3_client.put_object(
            Bucket=SHEIVA_SCRAPE_BUCKET,
            Key=bucket_dir,
            Body=json.dumps(bucket_contents),
        )
    print("Finished sending workout links to queue")
    return "Success"


# pylint: disable=unused-argument
def handler(event, context):
    """
    Lambda handler for scraping workout links.
    Args:
        event (Dict): event object
        context (Dict): context object
    """

    print("Received SQS event")
    boto3_session = boto3.Session()
    s3_client = boto3_session.client("s3")
    sqs_client = boto3_session.client("sqs")

    check_bucket_exists(s3_client=s3_client, bucket_name=SHEIVA_SCRAPE_BUCKET)
    workout_link_queue = StandardSQS(
        queue_url=WORKOUTLINK_QUEUE_URL, sqs_client=sqs_client
    )

    workout_scrape_trigger_messages = parse_sqs_message_data(
        sqs_body=event, parse_function=parse_workout_scrape_trigger_message
    )
    # Should only be one message
    (
        num_workout_links_to_scrape,
        receipt_handle,
    ) = workout_scrape_trigger_messages[0]

    workout_link_bucket_dirs = get_workout_link_bucket_dirs(
        s3_client=s3_client
    )

    get_and_post_workout_links(
        s3_client=s3_client,
        workout_link_queue=workout_link_queue,
        workout_link_bucket_dirs=workout_link_bucket_dirs,
        num_workout_links_to_scrape=num_workout_links_to_scrape,
    )

    print("Deleting workout scrape trigger message")
    workout_trigger_scrape_queue = StandardSQS(
        queue_url=WORKOUT_SCRAPE_TRIGGER_QUEUE_URL, sqs_client=sqs_client
    )
    workout_trigger_scrape_queue.delete_message(receipt_handle=receipt_handle)

    print("Finished scraping workout links")
