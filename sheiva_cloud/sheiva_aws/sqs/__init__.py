from sheiva_cloud.sheiva_aws.sqs.classes import (
    FileTransformerMessage,
    ParsedSqsMessage,
    ReceivedSqsMessage,
    ScraperMessage,
    SqsEvent,
    SqsResponse,
)

BASE_URL = "https://sqs.eu-west-1.amazonaws.com/381528172721"
WORKOUTLINK_QUEUE_URL = f"{BASE_URL}/WorkoutScraperQueue"
WORKOUTLINK_DEADLETTER_QUEUE_URL = f"{BASE_URL}/WorkoutScraperDeadLetterQueue"
WORKOUT_SCRAPE_TRIGGER_QUEUE_URL = f"{BASE_URL}/WorkoutScrapeTriggerQueue"
