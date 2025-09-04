import requests
from adapters.utils import canonicalize_url

def scrape(rec):
    # Oracle posts jobs via custom API (taleo legacy -> JSON)
    api = "https://oracle.taleo.net/careersection/rest/jobboard/searchjobs"
    payload = {"location":"United States", "limit":50}
    try:
        r = requests.post(api, json=payload, timeout=20)
        r.raise_for_status()
    except Exception:
        return []
    jobs = []
    for j in r.json().get("requisitionList", []):
        jobs.append({
            "id": j.get("Id",""),
            "title": j.get("Title",""),
            "location": j.get("Location",""),
            "apply_link": canonicalize_url(j.get("JobReqUrl","")),
            "posted_iso": j.get("PostedDate",""),
            "description": j.get("Description",""),
            "work_model": ""
        })
    return jobs
