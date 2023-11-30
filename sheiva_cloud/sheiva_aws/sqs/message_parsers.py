"""
Module for custom SQS utilities.
"""

import json
from typing import Tuple

from .classes import (
	ReceivedSqsMessage,
    ScraperMessage,
    FileTransformerMessage
)

def scrape_message_parser(
    message: ReceivedSqsMessage,
) -> ScraperMessage:
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


def file_transformer_message(
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
            "s3_input_file": message["messageAttributes"]["s3_input_file"][
                "stringValue"
            ],
            "s3_output_bucket_key": message["messageAttributes"][
                "s3_output_bucket_key"
            ]["stringValue"],
        }
    )


def workout_scrape_trigger_msg(
    message: ReceivedSqsMessage,
) -> Tuple[int, str]:
    """
    Parses the workout scrape trigger message.
    Args:
        message (Dict): message from the workout scrape trigger queue
    Returns:
        int: number of workout links to scrape
    """

    print("Parsing workout scrape trigger message")
    try:
        return int(message["body"]), message["receiptHandle"]
    # pylint: disable=broad-except
    except Exception as e:
        print(f"Error parsing workout scrape trigger message: {repr(e)}")
        return 0, ""
