import requests, json, re
from datetime import datetime, timezone
from adapters.utils import session, backoff_sleep, canonicalize_url

# Generic Workday CXS search
# We need tenant info; supply via rec["url"] or rec["tenant"] if present
# We'll try to infer tenant from URL when possible.

def infer_tenant(url):
    # examples: https://adobe.wd5.myworkdayjobs.com/en-US/external_experienced
    m = re.search(r"https?://([^.]+)\.wd\d?\.myworkdayjobs\.com", url)
    return m.group(1) if m else None

def cxs_endpoint(tenant, path=""):
    # '/wday/cxs/{tenant}/{site}/jobs'
    # We'll try common sites: External, external, careers, etc.
    return f"https://{tenant}.wd5.myworkdayjobs.com/wday/cxs/{tenant}/external/jobs"

def scrape(rec):
    url = rec["url"]
    tenant = rec.get("tenant") or infer_tenant(url)
    if not tenant:
        return []  # can't proceed
    ep = cxs_endpoint(tenant)
    s = session()
    jobs = []
    # Basic POST payload with no query brings first page; we paginate a bit
    start = 0
    page_size = 20
    for attempt in range(3):
        try:
            payload = {"appliedFacets":{}, "limit": page_size, "offset": start, "searchText": ""}
            r = s.post(ep, json=payload, timeout=20)
            if r.status_code != 200:
                break
            data = r.json()
            items = data.get("jobPostings", [])
            if not items: break
            for it in items:
                title = it.get("title","")
                locs = it.get("locationsText","") or it.get("locations", "")
                jid  = (it.get("bulletFields") or [{}])[0].get("text","") or it.get("externalPath","")
                posted_iso = it.get("postedOn", "")
                link = it.get("externalPath", "")
                if link and not link.startswith("http"):
                    link = f"https://{tenant}.wd5.myworkdayjobs.com{link}"
                jobs.append({
                    "id": jid,
                    "title": title,
                    "location": locs,
                    "apply_link": canonicalize_url(link),
                    "posted_iso": posted_iso,
                    "description": it.get("shortText","") or "",
                    "work_model": ""  # not reliable from CXS
                })
            if len(items) < page_size: break
            start += page_size
        except Exception:
            backoff_sleep(attempt)
            continue
    return jobs
