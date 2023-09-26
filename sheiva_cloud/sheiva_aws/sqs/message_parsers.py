"""
Module for custom SQS utilities.
"""

import json
from typing import Dict, List, TypedDict


class SQSMessage(TypedDict):
    """
    SQS Message structure.
    """

    receiptHandle: str


class ScraperMessage(SQSMessage):
    """
    SQS Message structure for scraping urls.
    urls: list of urls to scrape
    receiptHandle: receipt handle of the message
    bucket_key: key of the s3 bucket
    """

    urls: List[str]
    bucket_key: str


class FileTransformerMessage(SQSMessage):
    """
    SQS Message structure for a file transformer.
    s3_input_path: source directory of
        the file to be parsed.
    s3_output_path: destination directory
    """

    input_bucket_key: str
    output_bucket_key: str


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


def file_transformer_message_parser(message: Dict) -> FileTransformerMessage:
    """
    Parses a file transformer message from the SQS queue. Follows
    the FileTransformerMessage structure.
    Args:
        message (Dict): message from the SQS queue
    Returns:
        FileTransformerMessage: parsed message
    """

    return FileTransformerMessage(
        {
            "receiptHandle": message["receiptHandle"],
            "input_bucket_key": message["messageAttributes"][
                "input_bucket_key"
            ]["stringValue"],
            "output_bucket_key": message["messageAttributes"][
                "output_bucket_key"
            ]["stringValue"],
        }
    )
