"""
Script to bucket the full male workout link csv into
age buckets for the Kuda workout scraper. For local
use only.
"""

import os
import pandas as pd
import boto3
import json

from sheiva_cloud.sheiva_aws.s3 import (
    SHEIVA_SCRAPED_DATA_BUCKET as bucket_name,
)
from sheiva_cloud.sheiva_aws import get_boto3_session

workout_link_dir = "user-data/user-workout-links"

boto3_session = get_boto3_session()
s3_client = boto3_session.client("s3")


def get_all_workout_links() -> pd.DataFrame:
    res = s3_client.get_object(
        Bucket=bucket_name, Key=f"{workout_link_dir}/all_workout_links.csv"
    )
    print(
        f"Retrieved all workout links from s3 bucket: {workout_link_dir}/all_workout_links.csv"
    )
    csv_data = res["Body"].read().decode("utf-8")
    columns = csv_data.split("\n")[0].split("|")
    data = csv_data.split("\n")[1:-1]
    return pd.DataFrame(columns=columns, data=[d.split("|") for d in data])


def bucket_data() -> None:
    pdf = get_all_workout_links()
    pdf.Age = pdf.Age.apply(lambda x: int(x) if x not in ["--", ""] else -1)
    for x in range(5, 101, 5):
        print(f"Bucketing age {x - 5} to {x}")
        links = list(pdf[pdf.Age.between(x - 4, x)].Links)
        if links:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=f"{workout_link_dir}/age_{x - 5}_{x}.json",
                Body=json.dumps(links, indent=4),
            )
    links = list(pdf[pdf.Age == -1].Links)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"{workout_link_dir}/age_unknown.json",
        Body=json.dumps(links, indent=4),
    )


if __name__ == "__main__":
    bucket_data()
