# pylint: disable=all
# mypy: ignore-errors

"""
Script to bucket the full male workout link csv into
age buckets for the Kuda workout scraper. For local
use only.
"""

import json
from typing import Dict

import boto3
import pandas as pd

from sheiva_cloud import s3

GENDER = "male"

workout_link_dir = f"highrise/user-data/user-workout-links/{GENDER}"

# As of 13/09/2023 Male workout link counts
original_bucket_numbers = {
    "age_16_20": 235803,
    "age_21_25": 1215559,
    "age_26_30": 2193858,
    "age_31_35": 2197241,
    "age_36_40": 2208600,
    "age_41_45": 1220973,
    "age_46_50": 120,
    "age_51_55": 112032,
    "age_56_60": 153941,
    "age_61_65": 44694,
    "age_66_70": 14560,
    "age_71_75": 2225,
    "age_76_80": 150,
    "age_81_85": 51,
    "age_86_90": 322,
    "age_91_95": 229,
    "age_96_100": 1016,
    "age_unknown": 361885,
}


def get_all_workout_links(s3_client: boto3.client) -> pd.DataFrame:
    """
    Retrieves all workout links from s3.
    """

    res = s3_client.get_object(
        Bucket=s3.SHEIVA_SCRAPE_BUCKET,
        Key=f"{workout_link_dir}/all_workout_links.csv",
    )
    print(
        "Retrieved all workout links from s3 bucket: "
        f"{workout_link_dir}/all_workout_links.csv"
    )
    csv_data = res["Body"].read().decode("utf-8")
    columns = csv_data.split("\n")[0].split("|")
    data = csv_data.split("\n")[1:-1]
    return pd.DataFrame(columns=columns, data=[d.split("|") for d in data])


def bucket_data(s3_client: boto3.client) -> Dict:
    """
    Bucket the workout links into age groups of 5 years.
    Note: between means inclusive of the bounds.
    """

    bucket_numbers_dict = {}
    pdf = get_all_workout_links(s3_client=s3_client)
    pdf.Age = pdf.Age.apply(lambda x: int(x) if x not in ["--", ""] else -1)
    for x in range(5, 101, 5):
        group = f"age_{x - 4}_{x}"
        print(f"Bucketing group: {group}")
        links = list(pdf[pdf.Age.between(x - 4, x)].Links)
        if links:
            bucket_numbers_dict[f"{group}"] = len(links)
            s3_client.put_object(
                Bucket=s3.SHEIVA_SCRAPE_BUCKET,
                Key=f"{workout_link_dir}/{group}.json",
                Body=json.dumps(links, indent=4),
            )
    print("Bucketing group: age_unknown")
    links = list(pdf[pdf.Age == -1].Links)
    s3_client.put_object(
        Bucket=s3.SHEIVA_SCRAPE_BUCKET,
        Key=f"{workout_link_dir}/age_unknown.json",
        Body=json.dumps(links, indent=4),
    )
    bucket_numbers_dict["age_unknown"] = len(links)
    return bucket_numbers_dict


def get_all_workout_link_buckets(s3_client: boto3.client):
    print("Getting workout link bucket dirs")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=s3.SHEIVA_SCRAPE_BUCKET)
    return [
        f["Key"]
        for f in page_iterator.search(
            "Contents[?starts_with(Key, 'highrise/user-data/user-workout-links"
            f"/{GENDER}/') && ends_with(Key, '.json')]"
        )
    ]


if __name__ == "__main__":
    boto3_session = boto3.Session()
    s3_client = boto3_session.client("s3")

    workout_link_keys = get_all_workout_link_buckets(s3_client=s3_client)

    checked_buckets = {}
    for key in workout_link_keys:
        print("Checking bucket: ", key)
        res = s3_client.get_object(Bucket=s3.SHEIVA_SCRAPE_BUCKET, Key=key)
        obj = json.loads(res["Body"].read())

        bucket = key.split("/")[-1].split(".")[0]
        orginal_number = original_bucket_numbers[bucket]
        checked_buckets[bucket] = {
            "processed": orginal_number - len(obj),
            "left": len(obj),
            "bucket": bucket,
        }

    for bucket in original_bucket_numbers:
        if bucket not in checked_buckets:
            checked_buckets[bucket] = {
                "processed": original_bucket_numbers[bucket],
                "bucket": bucket,
                "left": 0,
            }

    print(
        "Total processed: ",
        sum([x["processed"] for x in checked_buckets.values()]),
    )
    print("Total left: ", sum([x["left"] for x in checked_buckets.values()]))
