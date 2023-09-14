import json
from typing import Callable, Dict, List, Optional, Tuple, TypedDict
from uuid import uuid4

import boto3
import requests

from sheiva_cloud.sheiva_aws.sqs.functions import parse_sqs_message_data
from sheiva_cloud.sheiva_aws.sqs.standard_sqs import StandardSQS


class ScraperMessage(TypedDict):
    """
    Message structure for scraping urls.
    """

    urls: List[str]
    receiptHandle: str
    s3_folder: Optional[str]


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
            "s3_folder": message["messageAttributes"]["s3_folder"][
                "stringValue"
            ],
        }
    )


def scrape_urls(
    urls: List[str], requests_session, scraper: Callable
) -> Tuple[List[Dict], List[str]]:
    """
    Scrapes a list of urls.
    Args:
        urls (List[str]): list of urls
        requests_session (requests.Session): requests session
        scraper (Callable): scraper function
    Returns:
        Tuple[List[Dict], List[str]]: scraped data and failed urls
    """

    failed_urls = []
    scraped_data = []
    for url in urls:
        try:
            scraped_data.append(
                scraper(url=url, requests_session=requests_session)
            )
        # pylint: disable=broad-except
        except Exception:
            failed_urls.append(url)
    return scraped_data, failed_urls


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


# pylint: disable=too-many-arguments
def process_scrape_event(
    event: Dict,
    bucket_name: str,
    bucket_key: str,
    main_queue_url: str,
    deadletter_queue_url: str,
    scraper: Callable,
):
    """
    Processes a scrape event.
    Args:
        event (Dict): event object
        bucket_name (str): name of the s3 bucket
        bucket_key (str): key of the s3 object
        main_queue_url (str): url of the main queue
        deadletter_queue_url (str): url of the deadletter queue
        scraper (Callable): scraper function
    """

    boto3_session = boto3.Session()
    s3_client = boto3_session.client("s3")
    sqs_client = boto3_session.client("sqs")
    requests_session = requests.Session()

    # A scraper lambda function should only receive one message
    # from the queue at a time.
    message = parse_sqs_message_data(
        sqs_body=event, parse_function=scrape_message_parser
    )[0]

    scraped_data, failed_scrapes = scrape_urls(
        urls=message["urls"],
        scraper=scraper,
        requests_session=requests_session,
    )

    s3_folder = message.get("s3_folder")
    if s3_folder:
        bucket_key = bucket_key.format(s3_folder, str(uuid4()))
    else:
        bucket_key = bucket_key.format(str(uuid4()))

    save_scraped_data_to_s3(
        s3_client=s3_client,
        bucket_name=bucket_name,
        key=bucket_key,
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
                "s3_folder": {
                    "DataType": "String",
                    "StringValue": s3_folder,
                }
            }
            if s3_folder
            else {},
        )
