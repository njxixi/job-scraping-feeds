import requests
from adapters.utils import canonicalize_url

def org_from(rec):
    # if user supplied boards URL, try to infer org; else expect rec["org"]
    url = rec["url"]
    if "boards.greenhouse.io" in url:
        parts = url.strip("/").split("/")
        org = parts[-1] if parts else None
        return org
    return rec.get("org")

def scrape(rec):
    org = org_from(rec)
    if not org: 
        return []
    api = f"https://boards-api.greenhouse.io/v1/boards/{org}/jobs?content=true"
    r = requests.get(api, timeout=20)
    r.raise_for_status()
    out = []
    for j in r.json().get("jobs", []):
        loc = (j.get("location") or {}).get("name","")
        url = canonicalize_url(j.get("absolute_url",""))
        out.append({
            "id": str(j.get("id","")),
            "title": j.get("title",""),
            "location": loc,
            "apply_link": url,
            "posted_iso": j.get("updated_at") or j.get("created_at") or "",
            "description": j.get("content","") or "",
            "work_model": ""
        })
    return out
