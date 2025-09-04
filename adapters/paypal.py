import requests
from adapters.utils import canonicalize_url

def scrape(rec):
    # PayPal careers is powered by Greenhouse API but masked
    api = "https://boards-api.greenhouse.io/v1/boards/paypal/jobs?content=true"
    try:
        r = requests.get(api, timeout=20)
        r.raise_for_status()
    except Exception:
        return []
    jobs = []
    for j in r.json().get("jobs", []):
        jobs.append({
            "id": str(j.get("id","")),
            "title": j.get("title",""),
            "location": (j.get("location") or {}).get("name",""),
            "apply_link": canonicalize_url(j.get("absolute_url","")),
            "posted_iso": j.get("updated_at",""),
            "description": j.get("content",""),
            "work_model": ""
        })
    return jobs
