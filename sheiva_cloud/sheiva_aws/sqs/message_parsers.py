"""
Module for custom SQS utilities.
"""

import json

from sheiva_cloud.sheiva_aws.sqs import (
    FileTransformerMessage,
    ReceivedSqsMessage,
    ScraperMessage,
)


def scrape_message_parser(message: ReceivedSqsMessage) -> ScraperMessage:
    """
    Parses a scrape message from the SQS queue. Follows
    the ScraperMessage structure.
    Args:
        message (ReceivedSqsMessage): message from the SQS queue
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


def file_transformer_message_parser(
    message: ReceivedSqsMessage,
) -> FileTransformerMessage:
    """
    Parses a file transformer message from the SQS queue. Follows
    the FileTransformerMessage structure.
    Args:
        message (ReceivedSqsMessage): message from the SQS queue
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
