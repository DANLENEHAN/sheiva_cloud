import json
from typing import Callable
from uuid import uuid4

import boto3
from kuda.scrapers import scrape_urls

from sheiva_cloud.sheiva_aws.s3 import SHEIVA_SCRAPE_BUCKET
from sheiva_cloud.sheiva_aws.sqs import ScraperMessage, SqsResponse


def process_scrape_event(
    message: ScraperMessage,
    html_parser: Callable,
    async_batch_size: int = 10,
) -> SqsResponse:
    """
    Processes a scrape event.
    Args:
        message (ScraperMessage): the message to be processed
        html_parser (Callable): html parser
        async_batch_size (int, optional): batch size for async scraping.
    """

    boto3_session = boto3.Session()
    s3_client = boto3_session.client("s3")

    results = scrape_urls(
        urls=message["urls"],
        html_parser=html_parser,
        batch_size=async_batch_size,
    )

    failed_scrapes = []
    scraped_data = []
    for result in results:
        if isinstance(result, str):
            failed_scrapes.append(result)
        else:
            scraped_data.append(result)

    bucket_key = message["bucket_key"]
    s3_client.put_object(
        Bucket=SHEIVA_SCRAPE_BUCKET,
        Key=f"{bucket_key}/{uuid4()}.json",
        Body=json.dumps(scraped_data, indent=4),
    )

    return {
        "receipt_handles_to_delete": [message["receiptHandle"]],
        "messages_to_dlq": [
            {
                "message_body": json.dumps(failed_scrapes),
                "message_attributes": {
                    "bucket_key": {
                        "DataType": "String",
                        "StringValue": bucket_key,
                    }
                },
            }
        ],
    }
