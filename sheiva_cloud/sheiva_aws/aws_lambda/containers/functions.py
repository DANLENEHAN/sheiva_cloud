import json
from typing import Callable, Dict
from uuid import uuid4

import boto3
from kuda.scrapers import scrape_urls

from sheiva_cloud.sheiva_aws.s3 import SHEIVA_SCRAPE_BUCKET
from sheiva_cloud.sheiva_aws.sqs.clients import StandardClient
from sheiva_cloud.sheiva_aws.sqs.message_parsers import scrape_message_parser
from sheiva_cloud.sheiva_aws.sqs.utils import parse_sqs_message_data


# pylint: disable=too-many-arguments, too-many-locals
def process_scrape_event(
    event: Dict,
    main_queue_url: str,
    deadletter_queue_url: str,
    html_parser: Callable,
    async_batch_size: int = 10,
):
    """
    Processes a scrape event.
    Args:
        event (Dict): event object
        main_queue_url (str): url of the main queue
        deadletter_queue_url (str): url of the deadletter queue
        html_parser (Callable): html parser
        async_batch_size (int, optional): batch size for async scraping.
    """

    boto3_session = boto3.Session()
    s3_client = boto3_session.client("s3")
    sqs_client = boto3_session.client("sqs")

    # A scraper lambda function should only receive one message
    # from the queue at a time.
    message = parse_sqs_message_data(
        sqs_body=event, parse_function=scrape_message_parser
    )[0]

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

    StandardClient(
        queue_url=main_queue_url, sqs_client=sqs_client
    ).delete_message(receipt_handle=message["receiptHandle"])

    # Send failed workout links to deadletter queue
    if failed_scrapes:
        StandardClient(
            queue_url=deadletter_queue_url,
            sqs_client=sqs_client,
        ).send_message(
            message_body=json.dumps(failed_scrapes),
            message_attributes={
                "bucket_key": {
                    "DataType": "String",
                    "StringValue": bucket_key,
                }
            }
            if bucket_key
            else {},
        )
