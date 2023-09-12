"""
Lambda function for scraping workout links.
Requires the following environment variables:
    - WORKOUTLINK_QUEUE_URL: url of the workout link SQS queue
    - SHEIVA_BUCKET: name of the s3 sheiva bucket
"""

import json
import os
from collections import defaultdict
from typing import Dict, List, TypedDict
from uuid import uuid4

import boto3
from kuda.scrapers import scrape_workout

from sheiva_cloud.sheiva_aws.s3.functions import (
    get_s3_client,
    check_bucket_exists,
)
from sheiva_cloud.sheiva_aws.sqs.functions import (
    get_sqs_queue,
    parse_sqs_message_data,
)
from sheiva_cloud.sheiva_aws.sqs.standard_sqs import StandardSQS

WORKOUTLINK_QUEUE_URL = os.getenv("WORKOUTLINK_QUEUE_URL", "")
SHEIVA_BUCKET = os.getenv("SHEIVA_BUCKET", "")


class WorkoutLinkMessage(TypedDict):
    """
    Workout link object.
    workout_link: The link to the workout
    receipt_handle: The receipt handle of the SQS message
    age_group_bucket_dir: The bucket the workout link should be stored
    """

    workout_link: str
    receipt_handle: str
    age_group_bucket_dir: str


def run_scraper(workout_link: str) -> Dict:
    """
    Scrapes a given workout link. Will return
    empty dict if any exception is raised.
    Args:
        workout_link (str): workout link
    Returns:
        Dict: workout data
    """

    try:
        return scrape_workout(workout_link)
    except Exception as e:
        print(
            f"Workout link: {workout_link} scrape exception caught: {e.__repr__()}"
        )
        return {}


def scrape_workouts(
    workout_link_messages: List[WorkoutLinkMessage], queue: StandardSQS
) -> Dict[str, List[Dict]]:
    """
    Scrapes all the workout links. Uploads any sucessful workouts to s3.
    Any unsuccessful workouts will be sent to the dead letter queue.
    Args:
        workout_link_messages (List[WorkoutLinkMessage]): list of workout link objects
        queue (StandardSQS): StandardSQS object
    """

    age_group_bucket_dir_dict = defaultdict(list)
    for message in workout_link_messages:
        workout_link = message["workout_link"]
        print(f"Attempting to scrape '{workout_link}'")
        workout_data = run_scraper(workout_link)
        if workout_data:
            print(f"Workout scrape of '{workout_link}' successful")
            age_group_bucket_dir_dict[message["age_group_bucket_dir"]].append(
                workout_data
            )
            queue.delete_message(receipt_handle=message["receipt_handle"])
        else:
            print(
                f"Workout scrape of '{workout_link}' unsuccessful with be sent to dead letter queue"
            )

    print("Finished scraping workouts")
    return age_group_bucket_dir_dict


def parse_sqs_workout_link_message(message: Dict) -> WorkoutLinkMessage:
    return WorkoutLinkMessage(
        {
            "workout_link": message["body"],
            "receipt_handle": message["receiptHandle"],
            "age_group_bucket_dir": message["messageAttributes"][
                "age_group_bucket_dir"
            ]["stringValue"],
        }
    )


def store_workout_data(
    s3_client: boto3.client,
    age_group_bucket_dir_dict: Dict[str, List[Dict]],
    sheiva_bucket: str,
) -> None:
    """
    Uploads scraped workout data to s3.
    Args:
        s3_client (boto3.client): boto3 client object
        age_group_bucket_dir_dict (Dict[str, List[Dict]]): dict of age group buckets
        sheiva_bucket (str): name of the s3 bucket
    """

    print(f"Uploading scraped workouts to s3")
    for age_group_dir, workouts in age_group_bucket_dir_dict.items():
        file_name = f"workout-data/{age_group_dir}/{uuid4().__str__()}.json"
        print(
            f"Uploading {len(workouts)} workouts to '{sheiva_bucket}/{file_name}'"
        )
        s3_client.put_object(
            Bucket=sheiva_bucket,
            Key=file_name,
            Body=json.dumps(workouts, indent=4),
        )


def handler(event, context):
    """
    Lambda handler for scraping workout links.
    Args:
        event (Dict): event object
        context (Dict): context object
    """

    print("Received SQS event")
    s3_client = get_s3_client()
    check_bucket_exists(s3_client=s3_client, bucket_name=SHEIVA_BUCKET)
    queue = get_sqs_queue(queue_name=WORKOUTLINK_QUEUE_URL)

    workout_link_messages = parse_sqs_message_data(
        sqs_body=event, parse_function=parse_sqs_workout_link_message
    )
    age_group_bucket_dir_dict = scrape_workouts(
        workout_link_messages=workout_link_messages,
        queue=queue,
    )

    store_workout_data(
        s3_client=s3_client,
        age_group_bucket_dir_dict=age_group_bucket_dir_dict,
        sheiva_bucket=SHEIVA_BUCKET,
    )

    print("Lambda function complete")
