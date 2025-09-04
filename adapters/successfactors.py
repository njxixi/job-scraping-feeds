from adapters.utils import soup, canonicalize_url
from datetime import datetime, timezone

# SuccessFactors often needs JS; we provide a best-effort HTML parser for simple boards.
# For companies where SF serves JSON (some do), you can upgrade this later.

def scrape(rec):
    url = rec["url"]
    try:
        s = soup(url)
    except Exception:
        return []
    out = []
    # heuristic: find anchors to job postings
    for a in s.select("a[href]"):
        href = a.get("href","")
        text = a.get_text(" ", strip=True)
        if not href or len(text) < 5: 
            continue
        if "job" in href.lower() or "job" in text.lower():
            loc = ""
            out.append({
                "id": "",
                "title": text,
                "location": loc,
                "apply_link": canonicalize_url(href if href.startswith("http") else url),
                "posted_iso": datetime.now(timezone.utc).isoformat(),
                "description": "",
                "work_model": ""
            })
    return out
