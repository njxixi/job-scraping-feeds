import pandas as pd
from datetime import datetime, timezone
import os

# Output files
TIER1_FILE = "tier1.csv"
TIER2_FILE = "tier2.csv"

# Output columns
COLUMNS = [
    "Tier", "Company", "Role Category", "Job Title", "Location",
    "Job ID/Req ID", "Direct Apply Link", "Posted/Updated (ISO)",
    "Work Model", "Notes"
]

def append_dummy():
    now_iso = datetime.now(timezone.utc).isoformat()

    # New dummy rows
    tier1_dummy = [[
        1, "Amazon", "Intern", f"Dummy SWE Intern {now_iso}", "Seattle, WA",
        f"TEST{int(datetime.now().timestamp())}", "https://www.amazon.jobs/en/jobs/TEST123",
        now_iso, "Hybrid", "Dummy entry for pipeline test"
    ]]

    tier2_dummy = [[
        2, "TestCorp", "Entry-Level", f"Dummy Data Analyst {now_iso}", "New York, NY",
        f"TEST{int(datetime.now().timestamp())}", "https://testcorp.com/jobs/TEST456",
        now_iso, "Remote", "Dummy entry for pipeline test"
    ]]

    # Append or create Tier 1
    if os.path.exists(TIER1_FILE):
        existing = pd.read_csv(TIER1_FILE)
        updated = pd.concat([existing, pd.DataFrame(tier1_dummy, columns=COLUMNS)], ignore_index=True)
    else:
        updated = pd.DataFrame(tier1_dummy, columns=COLUMNS)
    updated.to_csv(TIER1_FILE, index=False)

    # Append or create Tier 2
    if os.path.exists(TIER2_FILE):
        existing = pd.read_csv(TIER2_FILE)
        updated = pd.concat([existing, pd.DataFrame(tier2_dummy, columns=COLUMNS)], ignore_index=True)
    else:
        updated = pd.DataFrame(tier2_dummy, columns=COLUMNS)
    updated.to_csv(TIER2_FILE, index=False)

if __name__ == "__main__":
    append_dummy()
