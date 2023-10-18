"""
Lambda function for transforming a Highrise Workout json file into
four seperate csv files which aim to mimic the Grau ORM model structures
"""

import boto3

from sheiva_cloud import aws_lambda


# pylint: disable=unused-argument
def handler(event, context):
    """
    Lambda handler for scraping workout links.
    Args:
        event (Dict): event object
        context (Dict): context object
    """

    boto3_session = boto3.Session()
    s3_client = boto3_session.client("s3")

    aws_lambda.event_handlers.HighriseWorkoutTransformEvent(
        event=event, s3_client=s3_client
    ).process()
