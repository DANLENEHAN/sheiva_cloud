"""
Module for custom SQS utilities.
"""

import json
from typing import Tuple

from sheiva_cloud import sqs


def scrape_message_parser(
    message: sqs.ReceivedSqsMessage,
) -> sqs.ScraperMessage:
    """
    Parses a scrape message from the SQS queue. Follows
    the sqs.ScraperMessage structure.
    Args:
        message (sqs.ReceivedSqsMessage): message from the SQS queue
    Returns:
        sqs.ScraperMessage: parsed message
    """

    return sqs.ScraperMessage(
        {
            "urls": json.loads(message["body"]),
            "receiptHandle": message["receiptHandle"],
            "bucket_key": message["messageAttributes"]["bucket_key"][
                "stringValue"
            ],
        }
    )


def file_transformer_message(
    message: sqs.ReceivedSqsMessage,
) -> sqs.FileTransformerMessage:
    """
    Parses a file transformer message from the SQS queue. Follows
    the sqs.FileTransformerMessage structure.
    Args:
        message (sqs.ReceivedSqsMessage): message from the SQS queue
    Returns:
        sqs.FileTransformerMessage: parsed message
    """

    return sqs.FileTransformerMessage(
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
    message: sqs.ReceivedSqsMessage,
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
