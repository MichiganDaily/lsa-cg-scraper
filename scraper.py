import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
from datetime import datetime


TERM = "w_22_2370"
UG_SUBJ = "https://www.lsa.umich.edu/cg/cg_subjectlist.aspx?termArray={}&cgtype=ug&allsections=true".format(
    TERM
)
GR_SUBJ = "https://www.lsa.umich.edu/cg/cg_subjectlist.aspx?termArray={}&cgtype=gr&allsections=true".format(
    TERM
)

r = requests.get(UG_SUBJ)
soup = BeautifulSoup(r.text, features="html.parser")
ug_departments = set(
    [
        cell.text.strip()
        for cell in soup.select_one(".table.table-striped.table-condensed").select(
            "tr > td:nth-child(1)"
        )
    ]
)

r = requests.get(GR_SUBJ)
soup = BeautifulSoup(r.text, features="html.parser")
gr_departments = set(
    [
        cell.text.strip()
        for cell in soup.select_one(".table.table-striped.table-condensed").select(
            "tr > td:nth-child(1)"
        )
    ]
)

deps = {"ug": ug_departments, "gr": gr_departments}

course_sections = []
url = "https://www.lsa.umich.edu/cg/cg_results.aspx?termArray={}&cgtype={}&department={}&allsections=true&show=40"
for GRAD in ("ug", "gr"):
    for dep in deps[GRAD]:
        print(GRAD, dep)
        # Get each course listing from the department
        while True:
            try:
                r = requests.get(url.format(TERM, GRAD, dep), timeout=5)
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
                        timeout=5,
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
                a.strip() for a in result.select_one("font").text.strip().split("\r\n")
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


# Get ALL of the sections
all_sections = []

for name, url in tqdm(filtered.items()):
    while True:
        try:
            r = requests.get("https://www.lsa.umich.edu/cg/" + url, timeout=5)
        except requests.exceptions.Timeout:
            print("Retrying")
            continue
        break
    soup = BeautifulSoup(r.text, features="html.parser")
    for row in soup.select(".row.clsschedulerow"):
        row = row.select_one(".row")
        parts = [" ".join(a.text.strip().split()) for a in row.select(".col-md-1")]
        # turn into object
        obj = {"course": name, "time": datetime.now()}
        for part in parts:
            pieces = part.strip().split(":")
            key = pieces[0].strip()
            val = " ".join([p.strip() for p in pieces[1:]])
            obj[key] = val
        all_sections.append(obj)


df = pd.DataFrame(all_sections)
df.to_csv("./course_data.csv", index=False)
