"""
Lambda function for scraping workout links.
Requires the following environment variables:
    - MAIN_QUEUE: url of the workout link SQS queue
    - BUCKET: name of the s3 sheiva bucket
"""

import os

from kuda.scrapers import parse_workout_html

from sheiva_cloud.sheiva_aws.aws_lambda.containers.functions import process_scrape_event
from sheiva_cloud.sheiva_aws.sqs import (
    WORKOUTLINK_DEADLETTER_QUEUE_URL as DEADLETTER_QUEUE_URL,
)
from sheiva_cloud.sheiva_aws.sqs import WORKOUTLINK_QUEUE_URL as MAIN_QUEUE

# async batch size
ASYNC_BATCH_SIZE = int(os.getenv("ASYNC_BATCH_SIZE", "10"))


# pylint: disable=unused-argument
def handler(event, context):
    """
    Lambda handler for scraping workout links.
    Args:
        event (Dict): event object
        context (Dict): context object
    """

    process_scrape_event(
        event=event,
        main_queue_url=MAIN_QUEUE,
        deadletter_queue_url=DEADLETTER_QUEUE_URL,
        html_parser=parse_workout_html,
    )
