"""
Lambda function for scraping workout links.
Requires the following environment variables:
    - MAIN_QUEUE: url of the workout link SQS queue
    - BUCKET: name of the s3 sheiva bucket
"""

import os

import boto3
from kuda.scrapers import parse_workout_html

from sheiva_cloud.sheiva_aws import aws_lambda, sqs

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

    messages = sqs.utils.process_sqs_event(
        sqs_event=event,
        parse_function=sqs.message_parsers.scrape_message_parser,
    )

    sqs_response = aws_lambda.event_handlers.process_scrape_event(
        s3_client=s3_client,
        message=messages[0],
        html_parser=parse_workout_html,
    )

    sqs.utils.process_sqs_response(
        source_queue=sqs.StandardSqsClient(
            queue_url=sqs.WORKOUT_SCRAPER_QUEUE,
            sqs_client=sqs_client,
        ),
        dlq=sqs.StandardSqsClient(
            queue_url=sqs.WORKOUT_SCRAPER_DEADLETTER_QUEUE,
            sqs_client=sqs_client,
        ),
        sqs_response=sqs_response,
    )
