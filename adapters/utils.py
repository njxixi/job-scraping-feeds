import requests, re, time, random
from bs4 import BeautifulSoup
from urllib.parse import urlsplit, urlunsplit

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; JobScraperBot/1.0; +https://example.org/bot)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

COLUMNS = [
    "Tier","Company","Role Category","Job Title","Location","Job ID/Req ID",
    "Direct Apply Link","Posted/Updated Timestamp (ISO)","Work Model","Notes"
]

def session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s

def http_ok_and_has_apply(url, timeout=14):
    try:
        r = session().get(url, timeout=timeout, allow_redirects=True)
        if r.status_code != 200: return False
        html = r.text.lower()
        return ("apply" in html) or ("apply now" in html)
    except Exception:
        return False

def canonicalize_url(url):
    if not url: return url
    try:
        parts = list(urlsplit(url))
        # strip query + fragment
        parts[3] = ""
        parts[4] = ""
        return urlunsplit(parts)
    except Exception:
        return url

def soup(url, timeout=20):
    r = session().get(url, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def backoff_sleep(attempt):
    time.sleep(0.5 + 0.5*attempt + random.random()*0.5)

def dedupe_jobs(rows):
    # rows: list of dicts with COLUMNS
    seen = set()
    out = []
    for r in rows:
        key = (r.get("Company","").strip().lower(), r.get("Job ID/Req ID","").strip())
        if key in seen: 
            continue
        seen.add(key)
        out.append(r)
    return out
