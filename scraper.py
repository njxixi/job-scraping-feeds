import os
import csv
import json
from datetime import datetime
from adapters import amazon, workday, greenhouse, lever, successfactors
from filters import apply_filters

DATA_DIR = "data"

def write_to_csv(filename, rows):
    filepath = os.path.join(DATA_DIR, filename)
    file_exists = os.path.isfile(filepath)

    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "Tier", "Company", "Role Category", "Job Title", "Location",
                "Job ID/Req ID", "Direct Apply Link", 
                "Posted/Updated Timestamp (ISO)", "Work Model", "Notes"
            ])
        for row in rows:
            writer.writerow(row)

def scrape_companies(config_file, tier_label, output_csv):
    with open(os.path.join(DATA_DIR, config_file), "r", encoding="utf-8") as f:
        companies = json.load(f)

    all_rows = []
    for company in companies:
        scraper = {
            "workday": workday,
            "greenhouse": greenhouse,
            "lever": lever,
            "successfactors": successfactors,
            "amazon": amazon
        }.get(company["ats"])

        if not scraper:
            continue

        jobs = scraper.scrape(company["url"])
        jobs = apply_filters(jobs)

        for job in jobs:
            all_rows.append([
                tier_label,
                company["company"],
                job.get("role_category", ""),
                job.get("title", ""),
                job.get("location", ""),
                job.get("id", ""),
                job.get("apply_link", ""),
                job.get("posted", ""),
                job.get("work_model", ""),
                job.get("notes", "")
            ])

    if all_rows:
        write_to_csv(output_csv, all_rows)

if __name__ == "__main__":
    scrape_companies("tier1.json", "1", "tier1.csv")
    scrape_companies("fortune500.json", "2", "tier2.csv")

    # Log run history
    run_log = os.path.join(DATA_DIR, "run_history.csv")
    with open(run_log, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([datetime.utcnow().isoformat(), "Run completed"])
