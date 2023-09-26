"""
SQS Queue class for interacting with AWS SQS.

This is a 'Standard SQS queue (not FIFO)'. This means that
messages are not guaranteed to be delivered in the order they
are sent. If this queue has a dead letter queue (DLQ) attached
after the number of retries is exceeded, the message will be
sent to the DLQ. One of the benefits of using Standard SQS as
a lambda trigger is that you can batch up to 10,000 for standard
queues and 10 only for FIFO queues. You also don't need to worry
about message deduplication.
"""

from typing import Dict, Optional

import boto3


class StandardClient:
    """
    Client class for interacting with Standard
    Queue Service (SQS).
    """

    def __init__(
        self,
        queue_url: str,
        sqs_client: boto3.client,
    ):
        self.sqs_client = sqs_client
        self.queue_url = queue_url

    def send_message(
        self,
        message_body: str,
        message_attributes: Optional[Dict] = None,
    ) -> Dict:
        """
        Send a message to the queue.
        Args:
            message_body (str): The body of the message.
            message_attributes (dict): The message attributes.
        Returns:
            dict: The response from the SQS send_message method.
        """

        response = self.sqs_client.send_message(
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
                of messages to return. The default is 1.
        Returns:
            dict: The response from the SQS receive_message method.
        """

        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_number_of_messages,
            MessageAttributeNames=["All"],
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

        response = self.sqs_client.delete_message(
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

        response = self.sqs_client.purge_queue(QueueUrl=self.queue_url)
        return response
