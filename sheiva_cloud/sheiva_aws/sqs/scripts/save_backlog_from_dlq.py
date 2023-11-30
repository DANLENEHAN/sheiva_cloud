import json
from uuid import uuid4

import boto3

from sheiva_cloud.sheiva_aws import sqs


def main():
    """
    Main function for clearing the DLQ
    """

    boto3_session = boto3.Session()
    sqs_client = sqs.StandardSqsClient(
        queue_url=sqs.WORKOUT_SCRAPER_DEADLETTER_QUEUE,
        sqs_client=boto3_session.client("sqs"),
    )

    message_dicts = []
    messages = sqs_client.receive_message(max_number_of_messages=10)
    while messages.get("Messages"):
        print(f"Received {len(messages['Messages'])} messages")
        for message in messages["Messages"]:
            message_dicts.append(
                {
                    "body": json.loads(message["Body"]),
                    "bucket_key": message["MessageAttributes"]["bucket_key"][
                        "StringValue"
                    ],
                }
            )
            # Can avoid deleting if we want to test then purge the queue
            sqs_client.delete_message(message["ReceiptHandle"])
        messages = sqs_client.receive_message(max_number_of_messages=10)

    with open(f"dlq_backlog-{uuid4()}.json", "w", encoding="utf-8") as f:
        json.dump(message_dicts, f, indent=4)


if __name__ == "__main__":
    main()
