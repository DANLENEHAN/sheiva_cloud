"""
SQS Queue class for interacting with AWS SQS.

This is a 'Standard SQS queue (not FIFO)'. This means that
messages are not guaranteed to be delivered in the order they
are sent. If this queue has a dead letter queue (DLQ) attached
after the number of retries is exceeded, the message will be
sent to the DLQ. Unfortunately, the minimum number of retries
is 1 which actually means that the message will be sent to the
DLQ after the second attempt. This is because the first attempt
is the initial attempt and the second attempt is the first retry.
"""

from typing import Dict, Optional

import boto3


class SimpleSQS:
    """
    Class for interacting with Simple Queue Service (SQS).
    """

    def __init__(
        self,
        queue_url: str,
        boto3_session: Optional[boto3.Session] = None,
    ):
        self.sqs_session = boto3_session or boto3.Session().client("sqs")
        self.queue_url = queue_url

    def send_message(
        self,
        message_body: str,
        message_attributes: Optional[dict] = None,
    ) -> Dict:
        """
        Send a message to the queue.
        Args:
            message_body (str): The body of the message.
            message_attributes (dict): The message attributes.
        Returns:
            dict: The response from the SQS send_message method.
        """

        response = self.sqs_session.send_message(
            QueueUrl=self.queue_url,
            MessageBody=message_body,
            MessageAttributes=message_attributes or {},
        )
        return response

    def receive_message(
        self, max_number_of_messages: Optional[int] = 1
    ) -> Dict:
        """
        Receive messages from the queue.
        Args:
            max_number_of_messages (int): The maximum number
                of messages to return.
            We should be careful with requesting more than
                one message because deleting
            a message from the queue is not atomic. If we
                request more than one message.
        Returns:
            dict: The response from the SQS receive_message method.
        """

        response = self.sqs_session.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_number_of_messages,
        )
        return response

    def delete_message(self, receipt_handle: str) -> Dict:
        """
        Delete a message from the queue.
        Args:
            receipt_handle (str): The receipt handle of
                the message to delete. This is different to
                the message id. The receipt handle represents
                the specific instance of receiving the
                message versus a single message.
        Returns:
            dict: The response from the SQS delete_message method.
        """

        response = self.sqs_session.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
        )
        return response

    def purge_queue(self) -> Dict:
        """
        Purge all messages from the queue.
        Returns:
            dict: The response from the SQS purge_queue method.
        """

        response = self.sqs_session.purge_queue(QueueUrl=self.queue_url)
        return response
