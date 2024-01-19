import json

import boto3

from sheiva_cloud.sheiva_aws import sqs

BACKLOG_FILE_NAME = "dlq_backlog.json"

def main(sqs_client: boto3.client, backlog_file: str):
    """
    Pushes backlog to queue.
    """

    queue = sqs.StandardSqsClient(
        queue_url=sqs.WORKOUT_SCRAPER_QUEUE,
        sqs_client=sqs_client,
    )

    with open(backlog_file, "r", encoding="utf-8") as f:
        message_dicts = json.load(f)
    print(len(message_dicts))
    for _, message_dict in enumerate(message_dicts):
        queue.send_message(
            message_body=json.dumps(message_dict["body"]),
            message_attributes={
                "bucket_key": {
                    "DataType": "String",
                    "StringValue": message_dict["bucket_key"],
                }
            },
        )
    f.close()


if __name__ == "__main__":
    boto3_session = boto3.Session()
    main(
        sqs_client=boto3_session.client("sqs"),
        backlog_file=BACKLOG_FILE_NAME
    )
