from typing import Dict, Tuple

from kuda.scrapers import scrape_workout


def run_scraper(workout_link: str) -> Dict:
    try:
        return scrape_workout(workout_link)
    except Exception as e:
        print("Exception caught: ", e.__repr__())
        return {}


def parse_sqs_message(sqs_body: Dict) -> Tuple[str, str]:
    """
    Takes SQS message and extracts the body and
    receipt handle.
    Args:
            body (Dict): [description]
    Returns:
            [Dict]: [description]
    """
    messages = sqs_body["Records"]
    message = messages[0]

    # We should only be getting one message at a time
    if len(messages) > 1:
        raise ValueError("More than one message received")

    receipt_handle = message["receiptHandle"]
    body = message["body"]

    return body, receipt_handle


def handler(event, context):
    body, receipt_handle = parse_sqs_message(event)
    print(
        f"Received message body: {body} with receipt handle: {receipt_handle}"
    )
    response = run_scraper(body)
    if response:
        return response
    else:
        print("Put on DLQ")
