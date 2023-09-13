from typing import Callable, Dict, List

import boto3

from sheiva_cloud.sheiva_aws.sqs.standard_sqs import StandardSQS


def get_sqs_queue(
    queue_name: str, boto3_session: boto3.Session
) -> StandardSQS:
    """
    Connects to the WorkoutLinkMessage SQS queue.
    Returns:
        StandardSQS: StandardSQS object
    """

    print(f"Connecting to SQS queue, url: '{queue_name}'")
    queue = StandardSQS(
        boto3_session=boto3_session,
        queue_url=queue_name,
    )
    return queue


def parse_sqs_message_data(sqs_body: Dict, parse_function: Callable) -> List:
    """
    Takes SQS message event and extracts all the message bodies
    into a parsed list.
    Args:
        sqs_body (Dict): body of SQS message
        parse_function (Callable): function to parse message
    Returns:
        List: list of parsed messages
    """
    messages = sqs_body["Records"]

    print(
        f"Parsing {len(messages)} batched message"
        f"{'s' if len(messages) > 1 else ''}"
    )

    parsed_messages = []
    for message in messages:
        try:
            parsed_messages.append(parse_function(message))
        # pylint: disable=broad-except
        except Exception as e:
            print(
                f"Error parsing message: {message} "
                f"with exception: {repr(e)}"
            )
    return parsed_messages
