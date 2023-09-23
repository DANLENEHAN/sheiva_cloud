import json
from uuid import uuid4

import boto3

from sheiva_cloud.sheiva_aws.sqs import WORKOUTLINK_DEADLETTER_QUEUE_URL
from sheiva_cloud.sheiva_aws.sqs.standard_sqs import StandardSQS


def main():
    """
    Main function for clearing the DLQ
    """

    boto3_session = boto3.Session()
    sqs = StandardSQS(
        queue_url=WORKOUTLINK_DEADLETTER_QUEUE_URL,
        sqs_client=boto3_session.client("sqs"),
    )

    message_dicts = []
    messages = sqs.receive_message(max_number_of_messages=10)
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
        messages = sqs.receive_message(max_number_of_messages=10)
        # Don't delete the message for now just purge it's safer
        # sqs.delete_message(message["ReceiptHandle"])

    with open(f"dlq_backlog-{uuid4()}.json", "w") as f:
        json.dump(message_dicts, f, indent=4)


if __name__ == "__main__":
    main()
