import os, csv, json, sys, traceback
from datetime import datetime, timezone
from importlib import import_module
from adapters.utils import dedupe_jobs, COLUMNS
from filters import filter_and_normalize

DATA_DIR = "data"
TIER1_JSON = os.path.join(DATA_DIR, "tier1.json")
TIER2_JSON = os.path.join(DATA_DIR, "fortune500.json")
TIER1_CSV  = os.path.join(DATA_DIR, "tier1.csv")
TIER2_CSV  = os.path.join(DATA_DIR, "tier2.csv")
FIRST_SEEN = os.path.join(DATA_DIR, "first_seen.csv")
RUN_HIST   = os.path.join(DATA_DIR, "run_history.csv")

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_csv(path, header):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(header)

def append_rows(path, rows):
    if not rows: return 0
    ensure_csv(path, COLUMNS)
    # Load existing for append-only de-dupe
    existing = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            existing.append(row)
    # Combine and de-dupe by (Company, Job ID/Req ID)
    merged = existing + rows
    merged = dedupe_jobs(merged)
    # Only write if count increased
    if len(merged) > len(existing):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS)
            w.writeheader()
            for row in merged:
                w.writerow(row)
        return len(merged) - len(existing)
    return 0

def log_first_seen(companies):
    ensure_csv(FIRST_SEEN, ["Timestamp", "Tier", "Company"])
    seen = set()
    with open(FIRST_SEEN, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr: seen.add((row["Tier"], row["Company"]))
    ts = datetime.now(timezone.utc).isoformat()
    new_rows = []
    for c in companies:
        key = (str(c.get("tier")), c["company"])
        if key not in seen:
            new_rows.append({"Timestamp": ts, "Tier": str(c.get("tier")), "Company": c["company"]})
            seen.add(key)
    if new_rows:
        with open(FIRST_SEEN, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["Timestamp","Tier","Company"])
            for row in new_rows: w.writerow(row)
    return len(new_rows)

def log_run(tier1_added, tier2_added, tier1_companies, tier2_companies):
    ensure_csv(RUN_HIST, ["Timestamp","Tier1 New Jobs","Tier2 New Jobs","Tier1 Companies","Tier2 Companies"])
    with open(RUN_HIST, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            datetime.now(timezone.utc).isoformat(),
            tier1_added, tier2_added, tier1_companies, tier2_companies
        ])

def get_adapter(ats):
    # Map ats tag → module name in adapters/
    mapping = {
        "workday": "workday",
        "greenhouse": "greenhouse",
        "lever": "lever",
        "successfactors": "successfactors",
        "amazon": "amazon",
        "html": "site_html"   # generic HTML adapter
    }
    modname = mapping.get(ats, None)
    if not modname: return None
    try:
        return import_module(f"adapters.{modname}")
    except Exception:
        return None

def scrape_one_company(rec):
    """
    rec: {"company": "...", "tier": 1/2, "ats": "...", "url": "...", (optional extra config...)}
    Returns list of job dicts with the canonical COLUMNS keys.
    """
    adapter = get_adapter(rec.get("ats"))
    if not adapter:
        return []
    try:
        raw_jobs = adapter.scrape(rec)
        # Apply global filters + normalization
        norm = filter_and_normalize(raw_jobs, company=rec["company"], tier=str(rec.get("tier","")))
        return norm
    except Exception as e:
        print(f"[ERROR] {rec['company']}: {e}", file=sys.stderr)
        traceback.print_exc()
        return []

def scrape_tier(json_path, csv_path):
    companies = load_json(json_path)
    added = log_first_seen(companies)
    print(f"[First-Seen] Added {added} new companies to log.")
    all_new_rows = []
    for rec in companies:
        rows = scrape_one_company(rec)
        if rows:
            all_new_rows.extend(rows)
            print(f"[{rec['company']}] kept {len(rows)}")
    # Append-only write
    written = append_rows(csv_path, all_new_rows)
    return written, len(companies)

if __name__ == "__main__":
    t1_added, t1_comps = scrape_tier(TIER1_JSON, TIER1_CSV)
    t2_added, t2_comps = scrape_tier(TIER2_JSON, TIER2_CSV)
    log_run(t1_added, t2_added, t1_comps, t2_comps)
    print(f"[RUN] New rows: Tier1={t1_added}, Tier2={t2_added}")
