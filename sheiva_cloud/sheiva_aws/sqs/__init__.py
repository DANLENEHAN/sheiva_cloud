from typing import TypeVar

from sheiva_cloud.sheiva_aws.sqs.classes import (
    FileTransformerMessage,
    ParsedSqsMessage,
    ReceivedSqsMessage,
    ScraperMessage,
    SqsEvent,
    SqsResponse,
)

BASE_URL = "https://sqs.eu-west-1.amazonaws.com/381528172721"

WORKOUT_SCRAPER_QUEUE = f"{BASE_URL}/WorkoutScraperQueue"
WORKOUT_SCRAPER_DEADLETTER_QUEUE = f"{BASE_URL}/WorkoutScraperDeadLetterQueue"
WORKOUT_SCRAPER_TRIGGER_QUEUE = f"{BASE_URL}/WorkoutScraperTriggerQueue"

WORKOUT_FILE_TRANSFORM_QUEUE = f"{BASE_URL}/WorkoutFileTransformQueue"
WORKOUT_FILE_TRANSFORM_QUEUE_DEAD_LETTER_QUEUE = (
    f"{BASE_URL}/WorkoutFileTransformQueueDeadLetterQueue"
)

ParsedSqsMessageType = TypeVar("ParsedSqsMessageType", bound=ParsedSqsMessage)
