ATS_PATTERNS = {
    "workday": "myworkdayjobs.com",
    "greenhouse": "boards.greenhouse.io",
    "lever": "jobs.lever.co",
    "successfactors": "successfactors.com",
    "icims": "icims.com",
    "taleo": "taleo.net",
    "avature": "avature.net"
}

def detect_ats(url: str) -> str:
    url = (url or "").lower()
    for ats, pat in ATS_PATTERNS.items():
        if pat in url:
            return ats
    return "html"  # default to generic html adapter
