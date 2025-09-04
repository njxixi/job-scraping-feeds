import json
import os
import csv
import hashlib
from datetime import datetime, timezone
from importlib import import_module
from filters import filter_job

DATA_DIR = "data"

# -----------------------------
# HELPERS
# -----------------------------

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def stable_id(job: dict) -> str:
    """
    Ensure every job has a stable unique ID.
    Priority:
      1. Use job['id'] if present.
      2. Otherwise generate md5 hash of title+company+location.
    """
    if job.get("id"):
        return str(job["id"]).strip()

    base = f"{job.get('title','')}_{job.get('Company','')}_{job.get('location','')}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def secondary_hash(job: dict) -> str:
    """
    Backup dedupe key if job IDs are reused or missing.
    Based on title+company+location.
    """
    base = f"{job.get('title','')}_{job.get('Company','')}_{job.get('location','')}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def append_to_csv(path, rows, headers):
    """Append only new rows (dedupe by Job ID + secondary hash)"""
    seen_keys = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = f"{row['Job ID/Req ID']}|{row['Company']}|{row['Job Title']}|{row['Location']}"
                seen_keys.add(key)

    new_rows = []
    for r in rows:
        key = f"{r['Job ID/Req ID']}|{r['Company']}|{r['Job Title']}|{r['Location']}"
        if key not in seen_keys:
            seen_keys.add(key)
            new_rows.append(r)

    if not new_rows:
        return 0

    write_header = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)

    return len(new_rows)

def update_first_seen(path, companies):
    seen = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            seen = {line.strip().split(",")[0] for line in f if line.strip()}

    with open(path, "a", encoding="utf-8") as f:
        for comp in companies:
            if comp not in seen:
                f.write(f"{comp},{datetime.now(timezone.utc).isoformat()}\n")

def update_run_history(path, tier, count):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(timezone.utc).isoformat()},{tier},{count}\n")

def get_scraper(ats):
    try:
        return import_module(f"adapters.{ats}")
    except ModuleNotFoundError:
        return None

# -----------------------------
# MAIN SCRAPER
# -----------------------------

def run_for_tier(tier_name, json_file, csv_file):
    companies = load_json(os.path.join(DATA_DIR, json_file))
    all_jobs = []

    for rec in companies:
        scraper = get_scraper(rec["ats"])
        if not scraper:
            print(f"[WARN] No scraper for {rec['company']} (ATS={rec['ats']})")
            continue

        try:
            jobs = scraper.scrape(rec)
        except Exception as e:
            print(f"[ERROR] Failed {rec['company']}: {e}")
            continue

        for job in jobs:
            job["Tier"] = tier_name
            job["Company"] = rec["company"]

            filtered = filter_job(job)
            if filtered:
                job_id = stable_id(filtered)
                dedupe_key = secondary_hash(filtered)
                notes = filtered.get("notes", "")
                if not filtered.get("id"):
                    notes = (notes + " | " if notes else "") + "Synthetic ID generated from title+location"

                all_jobs.append({
                    "Tier": filtered["Tier"],
                    "Company": filtered["Company"],
                    "Role Category": filtered["role_category"],
                    "Job Title": filtered.get("title", ""),
                    "Location": filtered.get("location", ""),
                    "Job ID/Req ID": job_id,
                    "Direct Apply Link": filtered.get("apply_link", ""),
                    "Posted/Updated Timestamp (ISO)": filtered.get("posted_iso", ""),
                    "Work Model": filtered.get("work_model", ""),
                    "Notes": notes,
                    "Dedupe Key": dedupe_key,  # internal, not written to CSV
                })

    headers = [
        "Tier",
        "Company",
        "Role Category",
        "Job Title",
        "Location",
        "Job ID/Req ID",
        "Direct Apply Link",
        "Posted/Updated Timestamp (ISO)",
        "Work Model",
        "Notes"
    ]

    # Strip dedupe key before writing
    for job in all_jobs:
        job.pop("Dedupe Key", None)

    count = append_to_csv(os.path.join(DATA_DIR, csv_file), all_jobs, headers)
    update_first_seen(os.path.join(DATA_DIR, "first_seen.csv"), [rec["company"] for rec in companies])
    update_run_history(os.path.join(DATA_DIR, "run_history.csv"), tier_name, count)

    print(f"[INFO] {tier_name}: {count} new jobs added.")

def main():
    run_for_tier("Tier 1", "tier1.json", "tier1.csv")
    run_for_tier("Tier 2", "fortune500.json", "tier2.csv")

if __name__ == "__main__":
    main()
