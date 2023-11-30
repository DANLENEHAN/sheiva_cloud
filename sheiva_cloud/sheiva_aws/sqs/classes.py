from typing import Dict, List, TypedDict, TypeVar


class ReceivedSqsMessage(TypedDict):
    """
    Structure of an SQS message
    as received from an SQS event.
    """

    receiptHandle: str
    body: str
    messageAttributes: Dict


class SqsMessage(TypedDict):
    """
    Structure of an SQS message
    to send to a queue.
    """

    message_body: str
    message_attributes: Dict


class SqsEvent(TypedDict):
    """
    Structure of an SQS event.
    """

    Records: List[ReceivedSqsMessage]


class ParsedSqsMessage(TypedDict):
    """
    Base structure for a parsed SQs
    Message.
    """

    receiptHandle: str


class SqsResponse(TypedDict):
    """
    SqsResponse structure for
    finishing SQS Lambda events.
    receipt_handles_to_delete: receipt handles
        from messages be deleted from the source queue
    messages_to_dlq: messages to be sent
        to the dead-letter queue
    """

    receipt_handles_to_delete: List[str]
    messages_to_dlq: List[SqsMessage]


class ScraperMessage(ParsedSqsMessage):
    """
    Sqs Message structure for scraping urls.
    urls: list of urls to scrape
    receiptHandle: receipt handle of the message
    bucket_key: key of the s3 bucket
    """

    urls: List[str]
    bucket_key: str


class FileTransformerMessage(ParsedSqsMessage):
    """
    Sqs Message structure for a file transformer.
    s3_input_file: path of the source file to be parsed.
    s3_s3_output_bucket_key: destination for the transformed
        data.
    """

    s3_input_file: str
    s3_output_bucket_key: str

ParsedSqsMessageType = TypeVar("ParsedSqsMessageType", bound=ParsedSqsMessage)
