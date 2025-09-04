import re
from adapters.utils import soup, canonicalize_url
from datetime import datetime, timezone

def scrape(rec):
    # Amazon has JSON endpoints behind the search UI; we use HTML fallback to stay robust.
    url = "https://www.amazon.jobs/en/search?base_query=&loc_query=United%20States"
    try:
        s = soup(url)
    except Exception:
        return []
    out = []
    cards = s.select("div.job-tile, div.job-card, li.job")
    for c in cards:
        a = c.select_one("a[href]")
        if not a: 
            continue
        title = a.get_text(" ", strip=True)
        href  = a.get("href","")
        if href and href.startswith("/"):
            href = "https://www.amazon.jobs" + href
        loc = ""
        loc_el = c.select_one(".location, .loc, .job-location")
        if loc_el: loc = loc_el.get_text(" ", strip=True)
        # Try to extract Req ID
        jid = ""
        m = re.search(r"/jobs/([^/\s]+)", href or "")
        if m: jid = m.group(1)
        out.append({
            "id": jid,
            "title": title,
            "location": loc,
            "apply_link": canonicalize_url(href),
            "posted_iso": datetime.now(timezone.utc).isoformat(),  # HTML doesn’t expose consistently
            "description": c.get_text(" ", strip=True),
            "work_model": ""
        })
    return out
