import requests
from adapters.utils import canonicalize_url

def org_from(rec):
    url = rec["url"]
    if "jobs.lever.co" in url:
        parts = url.strip("/").split("/")
        org = parts[-1] if parts else None
        return org
    return rec.get("org")

def scrape(rec):
    org = org_from(rec)
    if not org:
        return []
    api = f"https://api.lever.co/v0/postings/{org}?mode=json"
    r = requests.get(api, timeout=20)
    r.raise_for_status()
    out = []
    for j in r.json():
        locs = j.get("categories", {}).get("location", "") or ""
        url = canonicalize_url(j.get("hostedUrl",""))
        out.append({
            "id": j.get("id",""),
            "title": j.get("text",""),
            "location": locs,
            "apply_link": url,
            "posted_iso": j.get("createdAt"),  # ms since epoch
            "description": (j.get("lists") or [{}])[0].get("text","") or "",
            "work_model": ""
        })
    # Normalize posted_iso (ms → ISO) in filters layer; here we just pass through
    return out
