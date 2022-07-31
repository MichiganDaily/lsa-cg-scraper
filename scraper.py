from tqdm.contrib.concurrent import process_map
from pickle import dump, load
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
import os

import boto3


TERM = "w_22_2370"
UG_SUBJ = "https://www.lsa.umich.edu/cg/cg_subjectlist.aspx?termArray={}&cgtype=ug&allsections=true".format(
    TERM
)
GR_SUBJ = "https://www.lsa.umich.edu/cg/cg_subjectlist.aspx?termArray={}&cgtype=gr&allsections=true".format(
    TERM
)


def slugify(text):
    return "-".join(text.lower().split())


def round_hour(time):
    return time.replace(second=0, microsecond=0, minute=0)


def get_departments():
    def parse_departments(url):
        r = requests.get(url)
        soup = BeautifulSoup(r.text, features="html.parser")
        departments = set(
            [
                cell.text.strip()
                for cell in soup.select_one(
                    ".table.table-striped.table-condensed"
                ).select("tr > td:nth-child(1)")
            ]
        )
        return departments

    return {"ug": parse_departments(UG_SUBJ), "gr": parse_departments(GR_SUBJ)}


def get_courses(deps):
    course_sections = []
    url = "https://www.lsa.umich.edu/cg/cg_results.aspx?termArray={}&cgtype={}&department={}&allsections=true&show=40"
    for GRAD in ("ug", "gr"):
        for dep in deps[GRAD]:
            print(GRAD, dep)
            # Get each course listing from the department
            while True:
                try:
                    r = requests.get(url.format(TERM, GRAD, dep), timeout=3)
                except requests.exceptions.Timeout:
                    print("Retrying")
                    continue
                break
            soup = BeautifulSoup(r.text, features="html.parser")
            results = soup.select(".row.result") + soup.select(".row.resultalt")
            while soup.select_one("#contentMain_hlnkNextBtm") is not None:
                print("getting next page")
                while True:
                    try:
                        r = requests.get(
                            "https://www.lsa.umich.edu/cg/"
                            + soup.select_one("#contentMain_hlnkNextBtm").get("href"),
                            timeout=3,
                        )
                        soup = BeautifulSoup(r.text, features="html.parser")
                        results += soup.select(".row.result") + soup.select(
                            ".row.resultalt"
                        )
                    except requests.exceptions.Timeout:
                        print("Retrying")
                        continue
                    break
            for result in results:
                # For each course listing, save the lecture course
                parts = [
                    a.strip()
                    for a in result.select_one("font").text.strip().split("\r\n")
                ]
                dept = parts[0]
                number = parts[1]
                name = " ".join(parts[3:])
                section, term, credits, mode, instructor, _ = [
                    a.text.strip()
                    for a in result.select_one(".bottompadding_main").find_all(
                        "div", recursive=False
                    )
                ]
                section = " ".join(section.split())
                term = " ".join(term.split()[1:])
                credits = credits.split()[-1]
                mode = mode.split()[-1]
                course_page = result.select_one("a").get("href")
                instructor = " ".join(instructor.split()[1:])
                course_sections.append(
                    {
                        "dept": dept,
                        "number": number,
                        "name": name,
                        "section": section,
                        "term": term,
                        "credits": credits,
                        "mode": mode,
                        "instructor": instructor,
                        "url": course_page,
                    }
                )
    filtered = dict()
    for c in course_sections:
        filtered[c["dept"] + " " + c["number"]] = c["url"]
    return filtered


def get_section(data):
    name, url = data
    while True:
        try:
            r = requests.get("https://www.lsa.umich.edu/cg/" + url, timeout=3)
        except requests.exceptions.Timeout:
            print(f"Retrying {name}")
            continue
        break
    soup = BeautifulSoup(r.text, features="html.parser")
    obj = None
    for row in soup.select(".row.clsschedulerow"):
        row = row.select_one(".row")
        parts = [" ".join(a.text.strip().split()) for a in row.select(".col-md-1")]
        # turn into object
        obj = {"Course": name, "Time": datetime.now()}
        for part in parts:
            pieces = part.strip().split(":")
            key = pieces[0].strip()
            val = " ".join([p.strip() for p in pieces[1:]])
            obj[key] = val
    if obj is None:
        print(f"{name} might not exist. Will be skipped in this output.")
    return obj

def get_all_sections(items):
    # Get ALL of the sections
    start = datetime.now()
    all_sections = process_map(get_section, items, max_workers=6)
    print(datetime.now() - start)
    return [section for section in all_sections if section is not None]


if __name__ == "__main__":
    bucket = "magnify.michigandaily.us"
    key = "data/course_data.csv"

    today = str(date.today())

    try:
        print("Trying to load from cache")
        filtered = load(open(f"{today}-courses.pkl", "rb"))
    except FileNotFoundError:
        print("Cache load failed; scraping courses")
        deps = get_departments()
        filtered = get_courses(deps)
        dump(filtered, open(f"{today}-courses.pkl", "wb"))

    print(f"Crawling {len(filtered)} courses")
    all_sections = get_all_sections(filtered.items())
    df = pd.DataFrame(all_sections)

    s3 = boto3.client("s3")
    try:
        data = s3.get_object(Bucket=bucket, Key=key)
        old_data = pd.read_csv(data["Body"])
        df = pd.concat([old_data, df])
    except s3.exceptions.NoSuchKey:
        print("No preexisting CSV")

    df.Time = pd.to_datetime(df.Time)
    df["Hour"] = df.Time.apply(round_hour)
    df["Open Seats"] = df["Open Seats"].astype(int)
    df["Wait List"] = df["Wait List"].apply(lambda x: int(x) if x != "-" else -1)

    df.to_csv("./course_data.csv", index=False)
    s3.upload_file(
        "./course_data.csv",
        bucket,
        key,
        ExtraArgs={
            "ContentType": "application/csv",
            "ACL": "public-read",
            "CacheControl": "max-age=3600",
        },
    )

    hourly_counts = (
        df[
            df.Section.str.contains("LEC")
            | df.Section.str.contains("SEM")
            | df.Section.str.contains("REC")
            | df.Section.str.contains("IND")
        ]
        .groupby(["Course", "Hour"])
        .agg("sum")
    )


    capacity = hourly_counts.groupby(level=0)["Open Seats"].agg("max")
    available = hourly_counts.groupby(level=0)["Open Seats"].agg("last")
    waitlist = hourly_counts.groupby(level=0)["Wait List"].agg("last")
    percent_available = available / capacity
    coursename = pd.Series(df.Course.unique(), index=df.Course.unique())
    undergrad = coursename.apply(lambda x: int(x.split()[1]) < 500)
    classnum = coursename.apply(lambda x: int(x.split()[1]))
    stdabrd = coursename.str.contains("STDABRD")
    dept = coursename.apply(lambda x: x.split()[0])
    slug = coursename.apply(slugify)

    overview = pd.DataFrame(
        {
            "Capacity": capacity,
            "Available": available,
            "Waitlist": waitlist,
            "PercentAvailable": percent_available,
            "Undergrad": undergrad,
            "Dept": dept,
            "CourseNum": classnum,
            "StudyAbroad": stdabrd,
        }
    )

    overview.dropna().to_csv("./overview.csv", index_label="Course", index=False)

    s3.upload_file(
        "./overview.csv",
        bucket,
        "course_data/overview.csv",
        ExtraArgs={
            "ContentType": "application/csv",
            "ACL": "public-read",
            "CacheControl": "max-age=3600",
        },
    )
    os.makedirs("./output/", exist_ok=True)
    for (course, listing) in df.groupby("Course").groups.items():
        df.loc[
            listing,
            [
                "Section",
                "Instruction Mode",
                "Class No",
                "Enroll Stat",
                "Open Seats",
                "Wait List",
                "Hour",
            ],
        ].to_csv(f"./output/{slugify(course)}.csv")

    for file in os.listdir("./output/"):
        s3.upload_file(
            f"./output/{file}",
            bucket,
            f"course_data/course/{file}",
            ExtraArgs={
                "ContentType": "application/csv",
                "ACL": "public-read",
                "CacheControl": "max-age=3600",
            },
        )
