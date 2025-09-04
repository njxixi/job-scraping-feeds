import requests
from adapters.utils import canonicalize_url

def scrape(rec):
    # Google’s careers site exposes JSON under /api/v3/search/jobs
    api = "https://careers.google.com/api/v3/search/jobs/"
    params = {"location": "United States", "company": "Google", "size": 50}
    r = requests.get(api, params=params, timeout=20)
    if r.status_code != 200:
        return []
    jobs = []
    for j in r.json().get("jobs", []):
        jobs.append({
            "id": j.get("id",""),
            "title": j.get("title",""),
            "location": j.get("locationsText",""),
            "apply_link": canonicalize_url(j.get("applyUrl","")),
            "posted_iso": j.get("published",""),
            "description": j.get("description",""),
            "work_model": ""
        })
    return jobs
