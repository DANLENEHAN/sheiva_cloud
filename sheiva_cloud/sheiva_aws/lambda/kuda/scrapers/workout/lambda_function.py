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

from sheiva_cloud.sheiva_aws.sqs.standard_sqs import StandardSQS


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


def parse_sqs_message_data(sqs_body: Dict) -> List[WorkoutLinkMessage]:
    """
    Takes SQS message event and extracts all the message bodies
    into a list.
    Args:
        sqs_body (Dict): body of SQS message
    Returns:
        List[WorkoutLinkMessage]: list of workout link messages
    """
    messages = sqs_body["Records"]

    print(
        f"Parsing {len(messages)} batched message{'s' if len(messages) > 1 else ''}"
    )

    workout_link_messages = []
    for message in messages:
        try:
            workout_link_messages.append(
                WorkoutLinkMessage(
                    {
                        "workout_link": message["body"],
                        "receipt_handle": message["receiptHandle"],
                        "age_group_bucket_dir": message["messageAttributes"][
                            "age_group_bucket_dir"
                        ]["stringValue"],
                    }
                )
            )
        except Exception as e:
            print(
                f"Error parsing message: {message} with exception: {e.__repr__()}"
            )
    return workout_link_messages


def get_sqs() -> StandardSQS:
    """
    Connects to the WorkoutLinkMessage SQS queue.
    Returns:
        StandardSQS: StandardSQS object
    """

    print("Connecting to SQS")
    queue = StandardSQS(
        boto3_session=boto3.Session(),
        queue_url=os.getenv("WORKOUTLINK_QUEUE_URL", ""),
    )
    return queue


def get_s3_connection(bucket_name: str) -> boto3.client:
    """
    Connects to the S3 bucket.
    Returns:
        boto3.client: boto3 client object
    """

    print(f"Connecting to S3 bucket: '{bucket_name}'")
    s3_client = boto3.Session().client("s3")
    try:
        s3_client.list_objects_v2(Bucket=bucket_name)
    except Exception as exp:
        raise Exception(
            f"Critical error: unable to connect to S3 bucket {bucket_name} with exception: {exp.__repr__()}"
        )
    return s3_client


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
    sheiva_bucket = os.getenv("SHEIVA_BUCKET")
    s3_client = get_s3_connection(bucket_name=sheiva_bucket)
    queue = get_sqs()

    workout_link_messages = parse_sqs_message_data(event)
    age_group_bucket_dir_dict = scrape_workouts(
        workout_link_messages=workout_link_messages,
        queue=queue,
    )

    store_workout_data(
        s3_client=s3_client,
        age_group_bucket_dir_dict=age_group_bucket_dir_dict,
        sheiva_bucket=sheiva_bucket,
    )

    print("Lambda function complete")
