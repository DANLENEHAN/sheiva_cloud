from typing import Dict, Optional

import boto3


class SQS:
    """
    Class for interacting with AWS SQS.
    Currenrtly our message deduplication
    scope is set to Message group so no need
    for message deduplication id.
    """

    def __init__(
        self,
        queue_url: str,
        message_group_id: Optional[str] = None,
        boto3_session: Optional[boto3.Session] = None,
    ):
        self.sqs_session = boto3_session or boto3.Session().client("sqs")
        self.queue_url = queue_url
        self.message_group_id = message_group_id

    def send_message(
        self,
        message_body: str,
        message_attributes: Optional[dict] = None,
        message_group_id: Optional[str] = None,
    ) -> Dict:
        """
        Send a message to the queue.
        Args:
            message_body (str): The body of the message.
            message_attributes (dict): The message attributes.
            message_group_id (str): The message group id.
        Returns:
            dict: The response from the SQS send_message method.
        """

        response = self.sqs_session.send_message(
            QueueUrl=self.queue_url,
            MessageBody=message_body,
            MessageAttributes=message_attributes,
            MessageGroupId=message_group_id or self.message_group_id,
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
