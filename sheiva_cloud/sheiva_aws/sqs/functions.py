from typing import Callable, Dict, List


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

    parsed_messages = []
    for message in sqs_body["Records"]:
        try:
            parsed_messages.append(parse_function(message))
        # pylint: disable=broad-except
        except Exception as e:
            print(
                f"Error parsing message: {message} "
                f"with exception: {repr(e)}"
            )
    return parsed_messages
