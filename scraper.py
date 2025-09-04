import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# Output files
TIER1_FILE = "tier1.csv"
TIER2_FILE = "tier2.csv"

# Define Tier 1 companies (expand this list)
TIER1_COMPANIES = {
    "Amazon": "https://www.amazon.jobs/en/search?base_query=intern+OR+%22new+grad%22+OR+%22entry+level%22+%22software%22+OR+%22data%20analyst%22&loc_query=United+States",
    "Microsoft": "https://jobs.careers.microsoft.com/global/en/search?q=intern%20OR%20%22new%20grad%22%20OR%20%22entry%20level%22%20data%20analyst%20OR%20software&lc=United%20States",
}

# Headers for output
COLUMNS = [
    "Tier", "Company", "Role Category", "Job Title", "Location",
    "Job ID/Req ID", "Direct Apply Link", "Posted/Updated (ISO)",
    "Work Model", "Notes"
]

def scrape_amazon(url):
    jobs = []
    resp = requests.get(url, timeout=20)
    soup = BeautifulSoup(resp.text, "html.parser")
    for card in soup.select("div.job-tile"):
        title = card.select_one("h3").get_text(strip=True)
        link = "https://www.amazon.jobs" + card.select_one("a")["href"]
        loc = card.select_one("p.location").get_text(strip=True)
        jid = link.split("/")[-1]
        role_cat = "Intern" if "intern" in title.lower() else "Entry-Level"
        jobs.append([
            1, "Amazon", role_cat, title, loc, jid, link,
            datetime.now(timezone.utc).isoformat(), "", ""
        ])
    return jobs

def scrape_microsoft(url):
    jobs = []
    resp = requests.get(url, timeout=20)
    soup = BeautifulSoup(resp.text, "html.parser")
    for card in soup.select("div.job-listing"):
        title = card.select_one("h2").get_text(strip=True)
        link = card.select_one("a")["href"]
        loc = card.select_one("span.location").get_text(strip=True)
        jid = link.split("/")[-1]
        role_cat = "Intern" if "intern" in title.lower() else "Entry-Level"
        jobs.append([
            1, "Microsoft", role_cat, title, loc, jid, link,
            datetime.now(timezone.utc).isoformat(), "", ""
        ])
    return jobs

def main():
    all_tier1 = []

    all_tier1.extend(scrape_amazon(TIER1_COMPANIES["Amazon"]))
    all_tier1.extend(scrape_microsoft(TIER1_COMPANIES["Microsoft"]))

    # Convert to DataFrame
    df1 = pd.DataFrame(all_tier1, columns=COLUMNS)
    df1.to_csv(TIER1_FILE, index=False)

    # Placeholder Tier 2 example (empty for now)
    df2 = pd.DataFrame(columns=COLUMNS)
    df2.to_csv(TIER2_FILE, index=False)

if __name__ == "__main__":
    main()
