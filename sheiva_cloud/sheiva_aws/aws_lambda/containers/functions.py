import json
from typing import Callable, Dict, List, TypedDict
from uuid import uuid4

import boto3
from kuda.scrapers import scrape_urls

from sheiva_cloud.sheiva_aws.sqs.functions import parse_sqs_message_data
from sheiva_cloud.sheiva_aws.sqs.standard_sqs import StandardSQS

SHEIVA_SCRAPE_DATA_BUCKET = "sheiva-scraped-data"


class ScraperMessage(TypedDict):
    """
    Message structure for scraping urls.
    urls: list of urls to scrape
    receiptHandle: receipt handle of the message
    bucket_key: key of the s3 bucket
    """

    urls: List[str]
    receiptHandle: str
    bucket_key: str


def scrape_message_parser(message: Dict) -> ScraperMessage:
    """
    Parses a scrape message from the SQS queue. Follows
    the ScraperMessage structure.
    Args:
        message (Dict): message from the SQS queue
    Returns:
        ScraperMessage: parsed message
    """

    return ScraperMessage(
        {
            "urls": json.loads(message["body"]),
            "receiptHandle": message["receiptHandle"],
            "bucket_key": message["messageAttributes"]["bucket_key"][
                "stringValue"
            ],
        }
    )


def save_scraped_data_to_s3(
    s3_client: boto3.client,
    bucket_name: str,
    key: str,
    data: List[Dict],
) -> None:
    """
    Saves scraped data to s3.
    Args:
        s3_client (boto3.client): s3 client
        bucket_name (str): name of the s3 bucket
        key (str): key of the s3 object
        data (List[Dict]): scraped data
    """

    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data, indent=4),
    )


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
    save_scraped_data_to_s3(
        s3_client=s3_client,
        bucket_name=SHEIVA_SCRAPE_DATA_BUCKET,
        key=f"{bucket_key}/{uuid4()}.json",
        data=scraped_data,
    )

    StandardSQS(
        queue_url=main_queue_url, sqs_client=sqs_client
    ).delete_message(receipt_handle=message["receiptHandle"])

    # Send failed workout links to deadletter queue
    if failed_scrapes:
        StandardSQS(
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
