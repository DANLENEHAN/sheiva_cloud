import json
import boto3

from sheiva_cloud.sheiva_aws.sqs import WORKOUTLINK_QUEUE_URL
from sheiva_cloud.sheiva_aws.sqs.standard_sqs import StandardSQS


def main(sqs_client: boto3.client, backlog_file: str):
	queue = StandardSQS(
		queue_url=WORKOUTLINK_QUEUE_URL,
		sqs_client=sqs_client,
	)

	f = open(backlog_file, "r")
	message_dicts = json.load(f)
	print(len(message_dicts))
	for index, message_dict in enumerate(message_dicts):
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
	sqs_client = boto3_session.client("sqs")
	main(
		sqs_client=sqs_client,
		backlog_file="dlq_backlog-055f4b6d-9205-4787-a3fe-bf86d62d6c5d.json"
	)
