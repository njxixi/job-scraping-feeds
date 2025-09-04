import re
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtp
from adapters.utils import http_ok_and_has_apply, canonicalize_url, COLUMNS

# US location heuristics (inclusive of onsite/hybrid/remote as long as US-based)
US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS",
    "KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY",
    "NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
}
US_PAT = re.compile(r"(united states|usa|\bUS\b|,\s*USA|,\s*United States)", re.I)

VISANEG_PAT = re.compile(
    r"(no sponsorship|unable to sponsor|cannot sponsor|citizens only|u\.s\. citizens only|us citizens only|green\s*card\s*only|gc\s*only|security\s*clearance|active\s*clearance)",
    re.I
)
EXP_OK_PAT  = re.compile(r"(intern|new\s*grad|entry[-\s]*level|junior|jr\.|0[-–]\s*2\s*years|\b1\s*year\b|\b2\s*years\b)", re.I)
EXP_BAD_PAT = re.compile(r"(\b3\+\s*years|\b4\+\s*years|\b5\+\s*years|\b[3-9]\s*years\b)", re.I)

def is_us_location(loc: str) -> bool:
    if not loc: return False
    t = loc.strip()
    if US_PAT.search(t): return True
    # City, ST
    parts = [p.strip() for p in t.split(",")]
    if len(parts) >= 2 and parts[-1].upper() in US_STATES: return True
    # Remote US hints
    if re.search(r"(remote).*(us|united states|usa)", t, re.I): return True
    return False

def within_24h(iso_str_or_text: str) -> bool:
    if not iso_str_or_text: return False
    now = datetime.now(timezone.utc)
    try:
        dt = dtp.parse(iso_str_or_text)
        if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
        return (now - dt) <= timedelta(hours=24)
    except Exception:
        t = iso_str_or_text.lower()
        return any(s in t for s in ["today", "24 hour", "1 day"])

def normalize_role_category(title: str, desc: str) -> str:
    t = f"{title or ''} {desc or ''}".lower()
    if "intern" in t: return "Intern"
    if "new grad" in t or "university grad" in t or "college grad" in t: return "New Grad"
    if "entry" in t or "entry-level" in t or "junior" in t or "jr." in t: return "Entry-Level"
    if "co-op" in t or "co op" in t: return "Co-op"
    return "Entry-Level"

def filter_and_normalize(jobs, company: str, tier: str):
    """
    Input jobs are minimally structured dicts with fields:
    id, title, location, url/apply_link, posted_iso or posted_text, work_model (optional), description (optional)
    """
    kept = []
    for j in jobs:
        title = j.get("title","")
        location = j.get("location","")
        desc = j.get("description","") or ""
        posted = j.get("posted_iso") or j.get("posted_text") or j.get("posted","")
        apply_url = j.get("apply_link") or j.get("url")

        # Location
        if not is_us_location(location): 
            continue
        # Experience window
        text_for_exp = f"{title}\n{desc}"
        if not EXP_OK_PAT.search(text_for_exp) or EXP_BAD_PAT.search(text_for_exp):
            continue
        # Visa / clearance screen
        if VISANEG_PAT.search(desc):
            continue
        # Freshness
        if not within_24h(posted):
            continue

        # Validate apply link (prefer ATS link if available)
        direct_link = canonicalize_url(apply_url) if apply_url else ""
        notes = ""
        if direct_link and not http_ok_and_has_apply(direct_link):
            notes = f"fallback-url {direct_link} (apply not detected)"
            direct_link = ""  # per your rule

        role_category = normalize_role_category(title, desc)

        kept.append({
            "Tier": tier,
            "Company": company,
            "Role Category": role_category,
            "Job Title": title.strip(),
            "Location": location.strip(),
            "Job ID/Req ID": (j.get("id") or "").strip(),
            "Direct Apply Link": direct_link,
            "Posted/Updated Timestamp (ISO)": j.get("posted_iso") or "",
            "Work Model": j.get("work_model",""),
            "Notes": notes
        })
    return kept
