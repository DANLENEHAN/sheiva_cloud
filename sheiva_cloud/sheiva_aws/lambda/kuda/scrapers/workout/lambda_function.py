"""
Lambda function for scraping workout links.
Requires the following environment variables:
    - WORKOUTLINK_QUEUE_URL: url of the workout link SQS queue
    - S3_BUCKET_NAME: name of the s3 bucket
"""

import os
from typing import Dict, List, TypedDict
from uuid import uuid4

import boto3

from kuda.scrapers import scrape_workout
from sheiva_cloud.sheiva_aws.sqs.standard_sqs import StandardSQS


class WorkoutLink(TypedDict):
    """
    Workout link object.
    """

    workout_link: str
    receipt_handle: str


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
        print("Exception caught: ", e.__repr__())
        return {}


def scrape_workouts(
    workout_links: List[WorkoutLink], queue: StandardSQS
) -> List[Dict]:
    """
    Scrapes all the workout links. Uploads any sucessful workouts to s3.
    Any unsuccessful workouts will be sent to the dead letter queue.
    Args:
        workout_links (List[WorkoutLink]): list of workout link objects
        queue (StandardSQS): StandardSQS object
    """

    scraped_workouts = []
    for workout_link in workout_links:
        print(f"Attempted to scrap '{workout_link['workout_link']}'")
        workout_data = run_scraper(workout_link["workout_link"])
        if workout_data:
            print("Workout scrape successful")
            scraped_workouts.append(workout_data)
            queue.delete_message(receipt_handle=workout_link["receipt_handle"])
        else:
            print(
                "Workout scrape unsuccessful with be sent to dead letter queue"
            )

    print("Finished scraping workouts")
    return scraped_workouts


def parse_sqs_message_data(sqs_body: Dict) -> List[WorkoutLink]:
    """
    Takes SQS message and extracts all the bodies
    into a list.
    Args:
        sqs_body (Dict): body of SQS message
    Returns:
        List[WorkoutLink]: list of workout link objects
    """
    messages = sqs_body["Records"]

    print(
        f"Parsing {len(messages)} btached message{'s' if len(messages) > 1 else ''}"
    )

    workout_links = [
        WorkoutLink(
            {
                "workout_link": message["body"],
                "receipt_handle": message["receiptHandle"],
            }
        )
        for message in messages
    ]

    print(f"Found {len(workout_links)} workout links")

    return workout_links


def get_sqs() -> StandardSQS:
    """
    Connects to the WorkoutLink SQS queue.
    Returns:
        StandardSQS: StandardSQS object
    """

    print("Connecting to SQS")
    queue = StandardSQS(
        boto3_session=boto3.Session(),
        queue_url=os.getenv("WORKOUTLINK_QUEUE_URL"),
    )
    return queue


def get_s3_connection() -> boto3.client:
    """
    Connects to the S3 bucket.
    Returns:
        boto3.client: boto3 client object
    """

    s3_client = boto3.client("s3")
    try:
        s3_client.list_objects_v2(Bucket=os.getenv("S3_BUCKET_NAME"))
    except Exception as exp:
        raise ("Critical error: unable to connect to S3 bucket")
    return s3_client


def handler(event, context):
    """
    Lambda handler for scraping workout links.
    Args:
        event (Dict): event object
        context (Dict): context object
    """

    s3_client = get_s3_connection()
    print(
        f"Received event SQS event RequestId: {event['ResponseMetadata']['RequestId']}'"
    )
    workout_links = parse_sqs_message_data(event)
    scraped_workouts = scrape_workouts(
        workout_links=workout_links,
        queue=get_sqs(),
    )

    print("Uploading scraped workouts to s3")
    s3_client.put_object(
        Bucket=os.getenv("S3_BUCKET_NAME"),
        Key=f"{uuid4().__str__()}.json",
        Body=scraped_workouts,
    )
