from adapters.utils import soup, canonicalize_url
from datetime import datetime, timezone

def scrape(rec):
    url = rec["url"]
    try:
        s = soup(url)
    except Exception:
        return []
    out = []
    # generic heuristic: anchors containing 'job' or 'apply'
    anchors = s.select("a[href]")
    for a in anchors:
        t = a.get_text(" ", strip=True)
        href = a.get("href","")
        if not href or len(t) < 4: 
            continue
        if any(k in (t.lower() + " " + href.lower()) for k in ["job", "apply", "careers/search", "opening"]):
            link = href if href.startswith("http") else url
            out.append({
                "id": "",
                "title": t,
                "location": "",
                "apply_link": canonicalize_url(link),
                "posted_iso": datetime.now(timezone.utc).isoformat(),
                "description": "",
                "work_model": ""
            })
    return out
