"""
Lambda function for scraping workout links.
Requires the following environment variables:
    - MAIN_QUEUE: url of the workout link SQS queue
    - BUCKET: name of the s3 sheiva bucket
"""

import os

import boto3
from kuda.scrapers import parse_workout_html

from sheiva_cloud.sheiva_aws.aws_lambda.containers.functions import process_scrape_event
from sheiva_cloud.sheiva_aws.sqs import (
    WORKOUTLINK_DEADLETTER_QUEUE_URL as DEADLETTER_QUEUE,
)
from sheiva_cloud.sheiva_aws.sqs import WORKOUTLINK_QUEUE_URL as SOURCE_QUEUE
from sheiva_cloud.sheiva_aws.sqs.clients.standard import StandardClient
from sheiva_cloud.sheiva_aws.sqs.message_parsers import scrape_message_parser
from sheiva_cloud.sheiva_aws.sqs.utils import process_sqs_event, process_sqs_response

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

    boto3_session = boto3.Session()
    sqs_client = boto3_session.client("sqs")
    s3_client = boto3_session.client("s3")

    messages = process_sqs_event(
        sqs_event=event,
        parse_function=scrape_message_parser,
    )

    sqs_response = process_scrape_event(
        s3_client=s3_client,
        message=messages[0],
        html_parser=parse_workout_html,
    )

    process_sqs_response(
        source_queue=StandardClient(
            queue_url=SOURCE_QUEUE,
            sqs_client=sqs_client,
        ),
        dlq=StandardClient(
            queue_url=DEADLETTER_QUEUE,
            sqs_client=sqs_client,
        ),
        sqs_response=sqs_response,
    )
