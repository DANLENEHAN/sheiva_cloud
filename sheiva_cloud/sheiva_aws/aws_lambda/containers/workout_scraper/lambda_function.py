"""
Lambda function for scraping workout links.
Requires the following environment variables:
    - MAIN_QUEUE: url of the workout link SQS queue
    - BUCKET: name of the s3 sheiva bucket
"""

import os

from kuda.scrapers import scrape_workout as scraper
from sheiva_cloud.sheiva_aws.aws_lambda.event_functions import process_scrape_event

# Queue URLs
MAIN_QUEUE = os.getenv("MAIN_QUEUE", "")
DEADLETTER_QUEUE_URL = os.getenv("DEADLETTER_QUEUE_URL", "")

# S3 bucket
BUCKET = os.getenv("BUCKET", "")
BUCKET_KEY = "workout-data/{}/{}.json"


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
        bucket_name=BUCKET,
        bucket_key=BUCKET_KEY,
        main_queue_url=MAIN_QUEUE,
        deadletter_queue_url=DEADLETTER_QUEUE_URL,
        scraper=scraper,
    )
