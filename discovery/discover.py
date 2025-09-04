import requests, json, os
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from ats_patterns import detect_ats

DATA_DIR = "data"
OUTPUT   = os.path.join(DATA_DIR, "fortune500.json")

def looks_like_careers(html: str) -> bool:
    h = (html or "").lower()
    return any(k in h for k in ["search jobs", "openings", "apply", "careers", "opportunities"])

def probe(base):
    candidates = ["", "/careers", "/jobs", "/company/careers", "/about/careers", "/careers/search"]
    for path in candidates:
        url = urljoin(base.rstrip("/") + "/", path.lstrip("/"))
        try:
            r = requests.get(url, timeout=12)
            if r.status_code == 200 and looks_like_careers(r.text):
                return url
        except Exception:
            continue
    return None

def discover(seed_list):
    out = []
    for comp in seed_list:
        base = comp.get("domain")
        name = comp.get("name")
        if not base or not name:
            continue
        url = probe(base)
        if not url: 
            continue
        ats = detect_ats(url)
        out.append({"company": name, "tier": 2, "ats": ats, "url": url})
    return out

if __name__ == "__main__":
    # Example: mini seed; replace with your Fortune 500 seed domains.
    seed = [
        {"name": "PepsiCo", "domain": "https://www.pepsico.com"},
        {"name": "Capital One", "domain": "https://www.capitalone.com"},
    ]
    found = discover(seed)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(found, f, indent=2)
    print(f"Wrote {len(found)} records to {OUTPUT}")
