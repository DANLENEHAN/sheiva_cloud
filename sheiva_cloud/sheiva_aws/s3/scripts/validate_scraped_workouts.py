"""
A script used primarily for testing. It will poll the amount of scraped
workouts in each age group and also test for uniqueness of the workouts
via the link.
"""

import json
import sys
from collections import defaultdict

import boto3

from sheiva_cloud.sheiva_aws.s3 import SHEIVA_SCRAPE_BUCKET as bucket_name

scraped_workouts_dir = "workout-data/male"

boto3_session = boto3.Session()

s3_client = boto3_session.client("s3")

paginator = s3_client.get_paginator("list_objects_v2")
page_iterator = paginator.paginate(Bucket=bucket_name)
files = [
    f["Key"]
    for f in page_iterator.search(
        "Contents[?starts_with(Key, "
        f"'{scraped_workouts_dir}') && "
        "ends_with(Key, '.json')]"
    )
]

# Gathering all workouts
workouts_by_age_group = defaultdict(list)
for file in files:
    age_group = file.split("/")[1]
    bucket = s3_client.get_object(Bucket=bucket_name, Key=file)
    print(f"Retrieved {file} from s3 bucket: {bucket_name}")
    workouts = json.loads(bucket["Body"].read())
    workouts_by_age_group[age_group].extend(workouts)

# Soft validate workouts
all_workouts = [
    workout
    for workouts in workouts_by_age_group.values()
    for workout in workouts
]

# pylint: disable=unreachable,no-else-continue,line-too-long
for workout in all_workouts:
    if len(workout["workout_components"]) == 0:
        continue
        print(f"Workout {workout['url']} has no workout components")
    else:
        for workout_component in workout["workout_components"]:
            if len(workout_component["sets"]) == 0:
                continue
                print(
                    f"Workout {workout['url']} has workout component {workout_component['name']} with no sets"
                )
            else:
                for set_ in workout_component["sets"]:
                    if len(set_["set_components"]) == 0:
                        continue
                        print(
                            f"Workout {workout['url']} has set {set_['name']} with no exercises"
                        )
                    else:
                        for set_component in set_["set_components"]:
                            if (
                                "exercise_name" not in set_component
                                or not set_component["exercise_name"]
                            ):
                                print(
                                    f"Workout {workout['url']} has set {set_['name']} with invalid exercise name"
                                )


urls = [workout["url"] for workout in all_workouts]
if len(urls) == len(set(urls)):
    print(f"No duplicates found in the {len(urls)} workouts")
    print("Exiting...")
    sys.exit(0)


# Removing duplicates
for age_group, workouts in workouts_by_age_group.items():
    unique_links = []
    unique_workouts = []
    for workout in workouts:
        workout_link = workout["url"]
        if workout_link not in unique_links:
            unique_links.append(workout_link)
            unique_workouts.append(workout)
        else:
            print(f"Duplicate workout link found: {workout_link}")

    workouts_by_age_group[age_group] = unique_workouts

# Making sure there are no duplicates across all age groups
urls = [
    workout["url"]
    for workouts in workouts_by_age_group.values()
    for workout in workouts
]
assert len(urls) == len(set(urls))

# Saving them back to s3
# As of writing this there were no duplicates
