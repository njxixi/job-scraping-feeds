import json
import os
import csv
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

def append_to_csv(path, rows, headers):
    """Append only new rows by Job ID"""
    seen_ids = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                seen_ids.add(row["Job ID/Req ID"])

    new_rows = [r for r in rows if r["Job ID/Req ID"] not in seen_ids]

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
                f.write(f"{comp},{datetime.now(timezone.utc).date().isoformat()}\n")

def update_run_history(path, tier, count):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(timezone.utc).date().isoformat()},{tier},{count}\n")

def update_stats(path, tier, scraped, accepted, added):
    """Write daily stats row for each tier, ensuring headers exist (date only)."""
    from datetime import datetime
    import pytz

    LOCAL_TZ = pytz.timezone("America/Los_Angeles")
    today = datetime.now(LOCAL_TZ).date().isoformat()

    rows = []
    headers = ["Date", "Tier", "Scraped", "Accepted", "Added"]

    # Load existing rows (except today's row for this tier, which we'll replace)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not (row["Date"] == today and row["Tier"] == tier):
                    rows.append(row)

    # Add/replace today's row
    rows.append({
        "Date": today,
        "Tier": tier,
        "Scraped": scraped,
        "Accepted": accepted,
        "Added": added,
    })

    # Always rewrite the file with headers
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

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
    scraped_total = 0

    for rec in companies:
        scraper = get_scraper(rec["ats"])
        if not scraper:
            print(f"[WARN] No scraper for {rec['company']} (ATS={rec['ats']})")
            continue

        try:
            jobs = scraper.scrape(rec)
            scraped_total += len(jobs)
        except Exception as e:
            print(f"[ERROR] Failed {rec['company']}: {e}")
            continue

        for job in jobs:
            job["Tier"] = tier_name
            job["Company"] = rec["company"]

            filtered = filter_job(job)
            if filtered:
                all_jobs.append({
                    "Tier": filtered["Tier"],
                    "Company": filtered["Company"],
                    "Role Category": filtered["role_category"],
                    "Job Title": filtered.get("title", ""),
                    "Location": filtered.get("location", ""),
                    "Job ID/Req ID": filtered.get("id", ""),
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

    # Stats breakdown
    scraped = scraped_total
    accepted = len(all_jobs)
    added = count

    # Update logs
    update_first_seen(os.path.join(DATA_DIR, "first_seen.csv"), [rec["company"] for rec in companies])
    update_run_history(os.path.join(DATA_DIR, "run_history.csv"), tier_name, count)
    update_stats(os.path.join(DATA_DIR, "stats.csv"), tier_name, scraped, accepted, added)

    print(f"[INFO] {tier_name} — Scraped: {scraped} | Accepted: {accepted} | Added: {added}")


def main():
    run_for_tier("Tier 1", "tier1.json", "tier1.csv")
    run_for_tier("Tier 2", "fortune500.json", "tier2.csv")

if __name__ == "__main__":
    main()
