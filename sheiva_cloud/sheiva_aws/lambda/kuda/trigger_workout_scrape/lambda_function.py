"""
Lambda function for putting workout links into the workout link queue.
Requires the following environment variables:
    - WORKOUTLINK_QUEUE_URL
    - WORKOUT_SCRAPE_TRIGGER_QUEUE: url of the workout link SQS queue
    - WORKOUT_LINKS_BUCKET: name of the s3 bucket

	
TODO:

- Create SQS queue for workout links - DONE
- Parse message from SQS queue getting the number of links to scrape - DONE
- Get workout link buckets
- Divide the number of links to scrape by the number of buckets 'n'
- For each bucket, pop 'n' links and put them into the workout link queue
- Re-save the bucket without the popped links
- Send delete message to the workout scrape trigger queue

"""

from typing import Dict, List
import os

from sheiva_cloud.sheiva_aws.s3.functions import (
    get_s3_client,
    check_bucket_exists,
)
from sheiva_cloud.sheiva_aws.sqs.functions import (
    get_sqs_queue,
    parse_sqs_message_data,
)

WORKOUTLINK_QUEUE_URL = os.getenv("WORKOUTLINK_QUEUE_URL", "")
SHEIVA_BUCKET = os.getenv("SHEIVA_BUCKET", "")
WORKOUT_SCRAPE_TRIGGER_QUEUE = os.getenv("WORKOUT_SCRAPE_TRIGGER_QUEUE", "")


def parse_workout_scrape_trigger_message(message: Dict) -> int:
    """
    Parses the workout scrape trigger message.
    Args:
        message (Dict): message from the workout scrape trigger queue
    Returns:
        int: number of workout links to scrape
    """

    try:
        return int(message["Body"])
    except Exception as e:
        print(f"Error parsing workout scrape trigger message: {e.__repr__()}")
        return 0


def handler(event, context):
    """
    Lambda handler for scraping workout links.
    Args:
        event (Dict): event object
        context (Dict): context object
    """

    print("Received SQS event")
    s3_client = get_s3_client()

    check_bucket_exists(s3_client=s3_client, bucket_name=SHEIVA_BUCKET)
    workout_link_queue = get_sqs_queue(WORKOUTLINK_QUEUE_URL)

    workout_scrape_trigger_messages = parse_sqs_message_data(
        sqs_body=event, parse_function=parse_workout_scrape_trigger_message
    )
    # Should only be one message
    workout_scrape_trigger_messages = workout_scrape_trigger_messages[0]
