"""
Use Angela's notebook code to calculate rates for all sections.
"""

import pandas as pd
import math
import os
from datetime import datetime, timedelta
from tqdm.contrib.concurrent import process_map


def past_rate(sectionNumber, pastData):
    classData = pastData[pastData["Class No"] == sectionNumber]
    # this might be faster if we only sorted this once
    classData = classData.sort_values(by=["Hour"], ascending=False).reset_index(
        drop=True
    )
    datetime.strptime("2021-12-09 19:00:00", "%Y-%m-%d %H:%M:%S")
    for i in range(0, len(classData) - 1):
        days = 25
        if classData["Open Seats"][i] > 0:
            days = (
                datetime.strptime(classData["Hour"][i], "%Y-%m-%d %H:%M:%S")
                - datetime.strptime(
                    classData["Hour"][len(classData) - 1], "%Y-%m-%d %H:%M:%S"
                )
            ).days
        if days == 0:
            days = 25
        return (classData["Open Seats"].agg("max") - classData["Open Seats"][i]) / days
    return 0


def get_rates(slug):
    data = pd.read_csv(
        "https://magnify.michigandaily.us/course_data/course/{}".format(slug)
    )
    results = []
    for class_no in data["Class No"].unique():
        results.append((class_no, past_rate(class_no, data)))
    return results



def main():
    # this is the old overview
    Overview = pd.read_csv("overview.csv")
    results = []
    def get_slugs():
        for dept, num in zip(Overview.Dept, Overview.CourseNum):
            slug = f"{dept.lower()}-{num}.csv"
            yield slug

    results = process_map(get_rates, list(get_slugs()), max_workers=6)
    flattened = [item for ls in results for item in ls]
    df = pd.DataFrame(flattened, columns=["Class No", "Rate"])
    df.to_csv("rates.w22.csv", index=False)


if __name__ == "__main__":
    main()
