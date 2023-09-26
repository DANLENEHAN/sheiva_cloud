"""
Module for generic SQS utilities.
"""

from typing import Callable, Dict, List, Type

import boto3

from sheiva_cloud.sheiva_aws.sqs import ParsedSqsMessage, SqsEvent, SqsResponse


def process_sqs_event(
    sqs_event: SqsEvent, parse_function: Callable
) -> List[Type[ParsedSqsMessage]]:
    """
    Takes SQS message event and extracts all the message bodies
    into a parsed list.
    Args:
        sqs_body (SqsEvent): body of SQS message
        parse_function (Callable): function to parse message
    Returns:
        List: list of parsed messages
    """

    parsed_messages = []
    for message in sqs_event["Records"]:
        try:
            parsed_messages.append(parse_function(message))
        # pylint: disable=broad-except
        except Exception as e:
            print(
                f"Error parsing message: {message} "
                f"with exception: {repr(e)}"
            )
    return parsed_messages


def process_sqs_response(
    source_queue: boto3.client, dlq: boto3.client, sqs_response: SqsResponse
):
    """
    Function for processing an SQS response. Sending delete response to
    source queue for successfully process messages and sending any failed
    messages to the dead-letter queue.
    Args:
        source_queue (boto3.client): source_queue of the SQS event
        dlq (boto3.client): the dead-letter queue for the SQS event
        sqs_response (SqsResponse): the SQS response after processing the
            event.
    """

    for receipt_handle in sqs_response["receipt_handles_to_delete"]:
        source_queue.delete_message(receipt_handle=receipt_handle)

    for message in sqs_response["messages_to_dlq"]:
        dlq.send_message(**message)
