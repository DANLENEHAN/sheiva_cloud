"""
Lambda function for putting messages on the Workout Scraper Trigger SQS queue.
Requires the following environment variables:
    - NUMBER_WORKOUT_LINKS_PER_MESSAGE: number of workout
        links to put in each message
"""

import os

import boto3

from sheiva_cloud.sheiva_aws.sqs import WORKOUT_SCRAPER_TRIGGER_QUEUE
from sheiva_cloud.sheiva_aws.sqs.clients import StandardClient

NUMBER_WORKOUT_LINKS_PER_MESSAGE = os.getenv(
    "NUMBER_WORKOUT_LINKS_PER_MESSAGE", None
)


# pylint: disable=unused-argument
def handler(event, context):
    """
    Lambda handler for putting messages on the
    workout scraper trigger SQS queue.
    Args:
        event (Dict): event object
        context (Dict): context object
    """

    print("Received SQS event")
    boto3_session = boto3.Session()
    sqs_client = boto3_session.client("sqs")

    queue = StandardClient(
        queue_url=WORKOUT_SCRAPER_TRIGGER_QUEUE, sqs_client=sqs_client
    )

    if not NUMBER_WORKOUT_LINKS_PER_MESSAGE:
        raise ValueError(
            "'NUMBER_WORKOUT_LINKS_PER_MESSAGE' environment variable not set"
        )

    print(f"Putting message on queue: '{WORKOUT_SCRAPER_TRIGGER_QUEUE}'")
    print(
        "Number of workout links per message: "
        f"{NUMBER_WORKOUT_LINKS_PER_MESSAGE}"
    )
    queue.send_message(message_body=NUMBER_WORKOUT_LINKS_PER_MESSAGE)

    print("Finished scraping workout links")
