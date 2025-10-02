# Automate job scraping from Adzuna with validation, logging, and storage for weekly updates.

import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# ----------------------------
# Load API keys
# ----------------------------
load_dotenv()
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")

# ----------------------------
# Define folders
# ----------------------------
RAW_DATA_DIR = "./data/raw/"
LOGS_DIR = "./logs/"
today_str = datetime.now().strftime("%Y-%m-%d")
SAVE_PATH = os.path.join(RAW_DATA_DIR, today_str)
os.makedirs(SAVE_PATH, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# ----------------------------
# Fetch jobs from Adzuna
# ----------------------------
def fetch_jobs(role="data scientist", results_per_page=50):
    url = "https://api.adzuna.com/v1/api/jobs/us/search/1"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": results_per_page,
        "what": role,
        "content-type": "application/json",
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()["results"]
    else:
        print(f"Error fetching jobs: {response.status_code}")
        return []

# ----------------------------
# Validation
# ----------------------------
def validate_job(job):
    """Check that required fields exist and are valid."""
    if not job.get("title") or not job.get("description") or len(job["description"]) < 50:
        return False
    if not job.get("location") or not job["location"].get("display_name"):
        return False
    return True

# ----------------------------
# Main execution
# ----------------------------
def main():
    role = "data scientist"
    jobs = fetch_jobs(role=role, results_per_page=50)

    # Save raw jobs
    raw_file = os.path.join(SAVE_PATH, "jobs.json")
    with open(raw_file, "w") as f:
        json.dump(jobs, f, indent=2)
    print(f"Saved {len(jobs)} jobs to {raw_file}")

    # Validate jobs
    validated_jobs = [job for job in jobs if validate_job(job)]
    validated_file = os.path.join(SAVE_PATH, "jobs_validated.json")
    with open(validated_file, "w") as f:
        json.dump(validated_jobs, f, indent=2)
    print(f"Validated jobs: {len(validated_jobs)} out of {len(jobs)}")
    print(f"Saved validated jobs to {validated_file}")

    # ----------------------------
    # Logging
    # ----------------------------
    log_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "role": role,
        "fetched": len(jobs),
        "validated": len(validated_jobs),
        "raw_file": raw_file,
        "validated_file": validated_file,
    }
    log_file = os.path.join(LOGS_DIR, "scraper_log.json")

    # Append to log file
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            logs = json.load(f)
    else:
        logs = []

    logs.append(log_entry)

    with open(log_file, "w") as f:
        json.dump(logs, f, indent=2)

    print(f"Logged run to {log_file}")

if __name__ == "__main__":
    main()
