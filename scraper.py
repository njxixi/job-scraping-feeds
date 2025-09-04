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

def stable_job_id(job):
    """
    Generate a stable ID for a job.
    Prefer ATS-provided ID. If missing, fallback to hash of title+location+apply_link.
    """
    jid = job.get("id")
    if jid and jid.strip():
        return jid.strip()
    key = f"{job.get('title','')}|{job.get('location','')}|{job.get('apply_link','')}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]  # shorter hash

def append_to_csv(path, rows, headers):
    """Hybrid mode: append new rows, or rebuild if file is empty."""
    seen_ids = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                seen_ids.add(row["Job ID/Req ID"])

    new_rows = [r for r in rows if r["Job ID/Req ID"] not in seen_ids]

    write_header = not os.path.exists(path) or os.path.getsize(path) == 0
    if not new_rows and not write_header:
        return 0

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
                f.write(f"{comp},{datetime.now(timezone.utc).date()}\n")

def update_run_history(path, tier, count):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(timezone.utc).date()},{tier},{count}\n")

def update_stats(path, tier, scraped, accepted):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(timezone.utc).date()},{tier},{scraped},{accepted}\n")

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
    scraped_count = 0
    accepted_count = 0

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

        scraped_count += len(jobs)

        for job in jobs:
            job["Tier"] = tier_name
            job["Company"] = rec["company"]
            job["id"] = stable_job_id(job)

            filtered = filter_job(job)
            if filtered:
                accepted_count += 1
                all_jobs.append({
                    "Tier": filtered["Tier"],
                    "Company": filtered["Company"],
                    "Role Category": filtered["role_category"],
                    "Job Title": filtered.get("title", ""),
                    "Location": filtered.get("location", ""),
                    "Job ID/Req ID": filtered["id"],
                    "Direct Apply Link": filtered.get("apply_link", ""),
                    "Posted/Updated Timestamp (ISO)": filtered.get("posted_iso", ""),
                    "Work Model": filtered.get("work_model", ""),
                    "Notes": filtered.get("notes", ""),
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

    count = append_to_csv(os.path.join(DATA_DIR, csv_file), all_jobs, headers)
    update_first_seen(os.path.join(DATA_DIR, "first_seen.csv"), [rec["company"] for rec in companies])
    update_run_history(os.path.join(DATA_DIR, "run_history.csv"), tier_name, count)
    update_stats(os.path.join(DATA_DIR, "stats.csv"), tier_name, scraped_count, accepted_count)

    print(f"[INFO] {tier_name}: {scraped_count} scraped, {accepted_count} passed filters, {count} new jobs added.")

def main():
    run_for_tier("Tier 1", "tier1.json", "tier1.csv")
    run_for_tier("Tier 2", "fortune500.json", "tier2.csv")

if __name__ == "__main__":
    main()
