import re
import requests

# Common ATS signatures
ATS_PATTERNS = {
    "workday": [
        r"myworkdayjobs\.com",
        r"/wd1/"
    ],
    "greenhouse": [
        r"boards\.greenhouse\.io"
    ],
    "brassring": [
        r"search\.brassring\.com",
        r"krb-sjobs"
    ],
    "successfactors": [
        r"successfactors\.com",
        r"career[.]sf"
    ],
    "icims": [
        r"icims\.com"
    ],
    "lever": [
        r"jobs\.lever\.co"
    ],
    "smartrecruiters": [
        r"smartrecruiters\.com"
    ],
    "adobe": [  # Example custom adapter
        r"adobe\.com/careers"
    ],
    "meta": [
        r"metacareers\.com"
    ],
    "tesla": [
        r"tesla\.com/careers"
    ]
}

def detect_ats(url: str) -> str:
    """
    Detect ATS type from a careers page URL by:
    1. Checking against known regex patterns
    2. Falling back to page content inspection
    """
    # First check URL patterns
    for ats, patterns in ATS_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, url, re.IGNORECASE):
                return ats

    # If no match, fetch and inspect HTML
    try:
        resp = requests.get(url, timeout=10)
        html = resp.text.lower()

        for ats, patterns in ATS_PATTERNS.items():
            for pat in patterns:
                if re.search(pat, html):
                    return ats
    except Exception as e:
        print(f"[WARN] Could not fetch {url}: {e}")

    return "custom"  # fallback — needs custom adapter

def enrich_company_record(record: dict) -> dict:
    """
    Take a minimal company record {company, careers_url}
    and return {company, careers_url, ats}
    """
    ats_type = detect_ats(record["careers_url"])
    record["ats"] = ats_type
    return record

if __name__ == "__main__":
    # Example test
    test_recs = [
        {"company": "Meta", "careers_url": "https://www.metacareers.com/"},
        {"company": "Tesla", "careers_url": "https://www.tesla.com/careers"},
        {"company": "PepsiCo", "careers_url": "https://www.pepsicojobs.com/main"}
    ]

    for rec in test_recs:
        enriched = enrich_company_record(rec)
        print(enriched)
