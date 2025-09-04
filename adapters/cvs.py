import requests
from adapters.utils import canonicalize_url

def scrape(rec):
    api = "https://jobs.cvshealth.com/api/jobs"
    params = {"country":"US","limit":50}
    try:
        r = requests.get(api, params=params, timeout=20)
        r.raise_for_status()
    except Exception:
        return []
    jobs = []
    for j in r.json().get("jobs", []):
        jobs.append({
            "id": j.get("jobId",""),
            "title": j.get("title",""),
            "location": j.get("location",""),
            "apply_link": canonicalize_url(j.get("jobUrl","")),
            "posted_iso": j.get("datePosted",""),
            "description": j.get("descriptionTeaser",""),
            "work_model": ""
        })
    return jobs
