"""
Script to bucket the full male workout link csv into
age buckets for the Kuda workout scraper. For local
use only.
"""

import json
from pprint import pprint
from typing import Dict

import boto3
import pandas as pd

from sheiva_cloud.sheiva_aws.s3 import SHEIVA_SCRAPE_BUCKET as bucket_name

GENDER = "male"

workout_link_dir = f"user-data/user-workout-links/{GENDER}"

boto3_session = boto3.Session()
s3_client = boto3_session.client("s3")

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
}


def get_all_workout_links() -> pd.DataFrame:
    """
    Retrieves all workout links from s3.
    """

    res = s3_client.get_object(
        Bucket=bucket_name, Key=f"{workout_link_dir}/all_workout_links.csv"
    )
    print(
        "Retrieved all workout links from s3 bucket: "
        f"{workout_link_dir}/all_workout_links.csv"
    )
    csv_data = res["Body"].read().decode("utf-8")
    columns = csv_data.split("\n")[0].split("|")
    data = csv_data.split("\n")[1:-1]
    return pd.DataFrame(columns=columns, data=[d.split("|") for d in data])


def bucket_data() -> Dict:
    """
    Bucket the workout links into age groups of 5 years.
    Note: between means inclusive of the bounds.
    """

    bucket_numbers_dict = {}
    pdf = get_all_workout_links()
    pdf.Age = pdf.Age.apply(lambda x: int(x) if x not in ["--", ""] else -1)
    for x in range(5, 101, 5):
        group = f"age_{x - 4}_{x}"
        print(f"Bucketing group: {group}")
        links = list(pdf[pdf.Age.between(x - 4, x)].Links)
        if links:
            bucket_numbers_dict[f"{group}"] = len(links)
            s3_client.put_object(
                Bucket=bucket_name,
                Key=f"{workout_link_dir}/{group}.json",
                Body=json.dumps(links, indent=4),
            )
    print(f"Bucketing group: age_unknown")
    links = list(pdf[pdf.Age == -1].Links)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"{workout_link_dir}/age_unknown.json",
        Body=json.dumps(links, indent=4),
    )
    bucket_numbers_dict["age_unknown"] = len(links)
    return bucket_numbers_dict


if __name__ == "__main__":
    response = bucket_data()
    pprint(response)
