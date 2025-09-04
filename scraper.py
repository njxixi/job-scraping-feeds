import re
from datetime import datetime, timedelta, timezone

# -----------------------------
# CONFIG
# -----------------------------
MAX_EXPERIENCE_YEARS = 2
POSTING_WINDOW_HOURS = 24

# Regex for visa restrictions
BLOCKLIST_PATTERNS = [
    r"no\s+sponsorship",
    r"cannot\s+sponsor",
    r"without\s+visa",
    r"green\s*card\s+only",
    r"citizens\s+only",
    r"us\s+citizenship\s+required",
    r"security\s+clearance",
]

# Regex for *senior-level exclusions*
EXCLUDE_TITLE_PATTERNS = [
    r"\bstaff\b",
    r"\bsenior\b",
    r"\bprincipal\b",
    r"\b(principal|principle)\s+engineer\b",
    r"\bmember\s+of\s+technical\s+staff\b",
    r"\bMTS\b",   # short form for Member of Technical Staff
]

# Regex for role categories
ROLE_KEYWORDS = {
    "Intern": [r"\bintern(ship)?\b"],
    "New Grad": [r"new\s*grad", r"university\s+graduate"],
    "Entry-Level": [r"entry[-\s]*level"],
    "Junior": [r"\bjunior\b"],
    "Co-op": [r"\bco[- ]?op\b"],
}

# -----------------------------
# HELPERS
# -----------------------------

def is_recent(posted_iso: str) -> bool:
    """Check if job was posted in the last 24 hours"""
    if not posted_iso:
        return False
    try:
        dt = datetime.fromisoformat(posted_iso.replace("Z", "+00:00"))
        return dt >= datetime.now(timezone.utc) - timedelta(hours=POSTING_WINDOW_HOURS)
    except Exception:
        return False

def is_us_location(loc: str) -> bool:
    """Keep only US-based roles (onsite, hybrid, or remote-US)"""
    if not loc:
        return False
    loc_lower = loc.lower()
    return any([
        "united states" in loc_lower,
        "u.s." in loc_lower,
        "us" in loc_lower,
        "usa" in loc_lower,
        re.search(r"\b[A-Z]{2}\b", loc)  # state abbreviations like NY, CA
    ])

def passes_visa_filter(text: str) -> bool:
    """Reject if text explicitly blocks sponsorship"""
    if not text:
        return True
    for pat in BLOCKLIST_PATTERNS:
        if re.search(pat, text, re.IGNORECASE):
            return False
    return True

def exclude_senior_roles(text: str) -> bool:
    """Reject jobs that are senior/staff/principal/etc."""
    if not text:
        return True
    for pat in EXCLUDE_TITLE_PATTERNS:
        if re.search(pat, text, re.IGNORECASE):
            return False
    return True

def infer_role_category(title: str, description: str) -> str:
    """Infer role category (Intern, New Grad, Entry-Level, Junior, Co-op)"""
    text = f"{title} {description}".lower()
    for cat, patterns in ROLE_KEYWORDS.items():
        for pat in patterns:
            if re.search(pat, text):
                return cat
    return "Entry-Level"  # default fallback

def requires_low_experience(text: str) -> bool:
    """Check if description requires ≤ 2 years experience"""
    if not text:
        return True
    matches = re.findall(r"(\d+)\s*\+?\s*years?", text.lower())
    if matches:
        min_years = min(int(y) for y in matches)
        return min_years <= MAX_EXPERIENCE_YEARS
    return True

# -----------------------------
# MAIN FILTER PIPELINE
# -----------------------------

def filter_job(job: dict) -> dict or None:
    """
    Apply all filters to a single job dict.
    Expected job keys:
      id, title, location, apply_link, posted_iso, description, work_model
    """
    title = job.get("title", "")
    desc = job.get("description", "")
    loc = job.get("location", "")
    posted = job.get("posted_iso", "")

    # 1. US only
    if not is_us_location(loc):
        return None

    # 2. Freshness
    if not is_recent(posted):
        return None

    # 3. Visa filters
    if not passes_visa_filter(f"{title} {desc}"):
        return None

    # 4. Senior/staff exclusion
    if not exclude_senior_roles(title):
        return None

    # 5. Experience
    if not requires_low_experience(desc):
        return None

    # 6. Enrich with inferred role category
    job["role_category"] = infer_role_category(title, desc)

    return job
