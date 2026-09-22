#!/usr/bin/env python3
"""Download completed B2C Order Report(s) from Anchanto WMS and upload the
raw CSVs to GCS for BigQuery_Anchanto.py to process.

Report creation itself is handled by a separate existing automation - this
script only checks for reports already marked "completed" and fetches them.
Reprocessing an already-consumed report is harmless: BigQuery_Anchanto.py
names its output by the order date found inside the file, so re-uploading
the same report just overwrites the same output deterministically rather
than creating a duplicate.

Credentials are read from a .env file next to this script and are never
stored in the script itself.
"""
import os
import sys
import time

import requests
from google.cloud import storage

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")

API_BASE = "https://scrwms-api.anchanto.com"
WAREHOUSE = "SCR"

GCS_BUCKET = "bucket_som"
GCS_RAW_PREFIX = "sales_parquet/raw/primary/anchanto/source/raw"

POLL_INTERVAL_SECONDS = 120
POLL_MAX_MINUTES = 20


def load_env(path=ENV_PATH):
    if not os.path.exists(path):
        sys.exit(
            f"Missing credentials file: {path}\n"
            "Copy .env.example to .env and fill in ANCHANTO_EMAIL and ANCHANTO_PASSWORD."
        )
    env = {}
    with open(path, "r", encoding="utf-8-sig") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip().strip('"').strip("'")

    missing = [k for k in ("ANCHANTO_EMAIL", "ANCHANTO_PASSWORD") if not env.get(k)]
    if missing:
        sys.exit(f"{path} is missing values for: {', '.join(missing)}")
    return env


def login(email, password):
    resp = requests.post(
        f"{API_BASE}/api/login",
        json={"api_user": {"email": email, "password": password}},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    jwt = data.get("jwt")
    if not jwt:
        sys.exit("Login response did not include a jwt token - Anchanto's login response shape may have changed.")
    return jwt


def fetch_completed_reports(jwt):
    headers = {
        "Authorization": f"Bearer {jwt}",
        "warehouse": WAREHOUSE,
        "x-language": "en",
        "accept": "application/json",
    }
    resp = requests.get(
        f"{API_BASE}/api/v1/report_schedules",
        params={"page[size]": 50, "include": "report_occurrence,report_type,created_by,latest_report"},
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    reports = []
    for item in data.get("data", []):
        attrs = item.get("attributes", {})
        filename = attrs.get("filename") or ""
        state = attrs.get("state") or attrs.get("status")
        if state == "completed" and filename.startswith("B2C_Order_Report_") and filename.endswith(".csv"):
            reports.append({
                "filename": filename,
                "report_url": attrs.get("report_url"),
                "from_date": attrs.get("from_date"),
                "end_date": attrs.get("end_date"),
                "job_id": attrs.get("job_id"),
            })
    return reports


def main():
    env = load_env()
    jwt = login(env["ANCHANTO_EMAIL"], env["ANCHANTO_PASSWORD"])
    print("Logged in to Anchanto WMS.")

    deadline = time.time() + POLL_MAX_MINUTES * 60
    reports = fetch_completed_reports(jwt)
    while not reports and time.time() < deadline:
        print(f"No completed B2C Order Reports yet, retrying in {POLL_INTERVAL_SECONDS}s ...")
        time.sleep(POLL_INTERVAL_SECONDS)
        reports = fetch_completed_reports(jwt)

    if not reports:
        sys.exit(f"No completed B2C Order Reports found within {POLL_MAX_MINUTES} minutes.")

    print(f"\nFound {len(reports)} completed report(s):")
    for r in reports:
        print(f"  {r['filename']}  ({r['from_date']} .. {r['end_date']})")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    for r in reports:
        content = requests.get(r["report_url"], timeout=300).content
        dest = f"{GCS_RAW_PREFIX}/{r['filename']}"
        bucket.blob(dest).upload_from_string(content, content_type="text/csv")
        print(f"Uploaded {r['filename']} ({len(content):,} bytes) -> gs://{GCS_BUCKET}/{dest}")

    print("\nDone.")


if __name__ == "__main__":
    main()
