from crossref.restful import Works
from tqdm import tqdm
import re
import pandas as pd


document = pd.read_parquet("../data/raw_data/document.prqt")
works = Works()
doi_aff = {}
exception_idens = []

for doi in tqdm(document.doi):
    try:
        record = works.doi(doi)
        if record:
            if "issued" in record:
                date_info = record["issued"]["date-parts"][0]
            else:
                date_info = [float("nan"), float("nan")]

            aff_set = set()
            if "author" in record:
                for author in record["author"]:
                    if author["affiliation"]:
                        aff_set.add(author["affiliation"][0]["name"])

            doi_aff[doi] = {
                "affiliation_list": list(aff_set),
                "publication_date": date_info,
            }
        else:
            doi_aff[doi] = {
                "affiliation_list": [],
                "publication_date": [float("nan"), float("nan")],
            }
    except Exception as e:
        exception_idens.append(doi)
        print(f"Something went wrong for {doi}, error: {e}")


def parse_aff(x):
    if re.search(r"\b(?:[0-9A-Z]+\-)?\d{5}(?:\-[0-9A-Z]+)?\b", x) is not None:
        return re.findall(r"\b(?:[0-9A-Z]+\-)?\d{5}(?:\-[0-9A-Z]+)?\b", x)[-1]
    elif re.search(r"\b\d{3}\s?\d{3}\b", x) is not None:
        return re.findall(r"\b\d{3}\s?\d{3}\b", x)[-1]
    elif (
        re.search(
            r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})",
            x,
        )
        is not None
    ):
        return re.findall(
            r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})",
            x,
        )[-1]
    else:
        return ""


# TODO: rewrite postcodes search, export to docs_with_postcodes.prqt
