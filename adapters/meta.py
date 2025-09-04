import requests
from datetime import datetime, timezone
from adapters.utils import canonicalize_url

def scrape(rec):
    # Meta Careers GraphQL endpoint
    gql_url = "https://www.metacareers.com/graphql"
    query = {
        "operationName": "JobSearchResults",
        "variables": {
            "filters": {"location": "united-states"},
            "first": 50,
            "after": None
        },
        "query": """
        query JobSearchResults($filters: JobSearchFilters, $first: Int, $after: String) {
          jobs(filters: $filters, first: $first, after: $after) {
            edges {
              node {
                id
                title
                workLocation
                url
                datePosted
              }
            }
          }
        }
        """
    }
    jobs = []
    try:
        r = requests.post(gql_url, json=query, timeout=20)
        r.raise_for_status()
        data = r.json()
        for edge in data["data"]["jobs"]["edges"]:
            n = edge["node"]
            jobs.append({
                "id": n["id"],
                "title": n["title"],
                "location": n.get("workLocation",""),
                "apply_link": canonicalize_url(n["url"]),
                "posted_iso": n["datePosted"],
                "description": "",
                "work_model": ""
            })
    except Exception:
        return []
    return jobs
