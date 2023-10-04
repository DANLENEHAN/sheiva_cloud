import json
from typing import Callable, Dict, List, Any, Tuple
from uuid import uuid4
import pandas as pd

import boto3
from kuda.scrapers import scrape_urls

from kuda.data_pipelining.highrise.file_transformers import parse_workout_tree

from sheiva_cloud.sheiva_aws.s3 import SHEIVA_SCRAPE_BUCKET
from sheiva_cloud.sheiva_aws.sqs import (
    FileTransformerMessage,
    SqsEvent,
    ScraperMessage,
    SqsResponse,
)
from sheiva_cloud.sheiva_aws.sqs.message_parsers import file_transformer_message_parser
from sheiva_cloud.sheiva_aws.sqs.utils import process_sqs_event

class FileTransformEvent:
    """
    Represents a file transform event.
    """

    def __init__(
        self, event: SqsEvent, s3_client: boto3.client
    ):
        """
        Args:
            message (FileTransformerMessage): message
            s3_client (boto3.client): s3 client
        """

        self.s3_client = s3_client
        self.message = process_sqs_event(
            sqs_event=event,
            parse_function=file_transformer_message_parser,
        )[0]

    def process(self):
        """
        Processes the event.
        """
        pass

    def parse_source_file(self):
        """
        Parses the source file from the
        's3_input_file' of the message.
        """
        pass

    def store_parsed_results(self, file_name: str, parsed_results: Any):
        """
        Stores the parsed results in the
        's3_output_bucket_key' of the message.
        """
        pass


class HighriseWorkoutTransformEvent(FileTransformEvent):
    """
    Represents a highrise workout transform event.
    """

    def process(self) -> str:
        """
        Processes the event.
        """
        file_name, parsed_results = self.parse_source_file()
        self.store_parsed_results(
            file_name=file_name, parsed_results=parsed_results
        )
        return "Success"


    def parse_source_file(self) -> Tuple[str, Dict[str, List]]:
        """
        Extracts file name from the 's3_input_file' of the message.
        Parses the source file from the 's3_input_file' of the message.
        Returns:
            Tuple[str, Dict[str, List]]: file name and parsed results
        """
        file_name = self.message["s3_input_file"].split("/")[-1].split(".")[0]
        response = self.s3_client.get_object(
            Bucket=SHEIVA_SCRAPE_BUCKET, Key=self.message["s3_input_file"]
        )
        workouts = json.loads(response["Body"].read().decode("utf-8"))
        parsed_results = parse_workout_tree(workouts=workouts)
        return file_name, parsed_results

    def store_parsed_results(
        self, file_name: str, parsed_results: Dict[str, List]
    ) -> None:
        """
        Stores the parsed results in the 's3_output_bucket_key' of the message.
        Args:
            file_name (str): file name
            parsed_results (Dict[str, List]): parsed results
        Returns:
            str: success message
        """

        for component_key, components in parsed_results.items():
            bucket_key = f"{self.message['s3_output_bucket_key']}/{component_key}/{file_name}.csv"
            pd.DataFrame(components).to_csv(bucket_key, index=False)


def process_scrape_event(
    s3_client: boto3.client,
    message: ScraperMessage,
    html_parser: Callable,
    async_batch_size: int = 10,
) -> SqsResponse:
    """
    Processes a scrape event.
    Args:
        s3_client (boto3.client): s3 client
        message (ScraperMessage): the message to be processed
        html_parser (Callable): html parser
        async_batch_size (int, optional): batch size for async scraping.
    """

    results = scrape_urls(
        urls=message["urls"],
        html_parser=html_parser,
        batch_size=async_batch_size,
    )

    failed_scrapes = []
    scraped_data = []
    for result in results:
        if isinstance(result, str):
            failed_scrapes.append(result)
        else:
            scraped_data.append(result)

    bucket_key = message["bucket_key"]
    s3_client.put_object(
        Bucket=SHEIVA_SCRAPE_BUCKET,
        Key=f"{bucket_key}/{uuid4()}.json",
        Body=json.dumps(scraped_data, indent=4),
    )

    return {
        "receipt_handles_to_delete": [message["receiptHandle"]],
        "messages_to_dlq": [
            {
                "message_body": json.dumps(failed_scrapes),
                "message_attributes": {
                    "bucket_key": {
                        "DataType": "String",
                        "StringValue": bucket_key,
                    }
                },
            }
        ]
        if failed_scrapes
        else [],
    }
