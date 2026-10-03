#!/usr/bin/env python3
"""Download the newest B2C Order Report of every period from Anchanto WMS and
upload the raw CSVs to GCS.

Anchanto's "other automation" (outside this repo) creates one report per ~10-day
period. The open period is regenerated about every 2 hours as new orders arrive;
closed periods are regenerated once around 00:20 WIB. Each regeneration flips the
older report's `state` from "completed" to "deleted" (its `report_url` stays valid).

This pipeline runs every 6 hours, so each run takes, per period, the NEWEST report
created in the last ANCHANTO_LOOKBACK_HOURS (default 8) - searching both completed
and deleted reports. A period nothing regenerated in that window is skipped: its
data is already in GCS from an earlier run.

Credentials are read from a .env file next to this script and are never
stored in the script itself.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

import requests
from google.cloud import storage

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")

API_BASE = "https://scrwms-api.anchanto.com"
WAREHOUSE = "SCR"
WIB = timezone(timedelta(hours=7))

GCS_BUCKET = "bucket_som"
GCS_RAW_PREFIX = "sales_parquet/raw/primary/anchanto/source/raw"

PAGE_SIZE = 100
MAX_PAGES = 30  # entries come back newest-first; stop once we're past the window


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


LOOKBACK_HOURS = float(os.environ.get("ANCHANTO_LOOKBACK_HOURS", "8"))


def lookback_window_utc(now_utc=None):
    """The last LOOKBACK_HOURS, as UTC bounds."""
    now = now_utc or datetime.now(timezone.utc)
    return now - timedelta(hours=LOOKBACK_HOURS), now


def fetch_reports(jwt, state, window_start):
    """Page through report_schedules for a given state (None = default/
    completed view), stopping once entries are clearly older than the
    target window (they come back newest-created-first)."""
    headers = {
        "Authorization": f"Bearer {jwt}",
        "warehouse": WAREHOUSE,
        "x-language": "en",
        "accept": "application/json",
    }
    stop_before = window_start - timedelta(hours=1)  # margin of safety
    items = []
    for page in range(1, MAX_PAGES + 1):
        params = {"page[size]": PAGE_SIZE, "page[number]": page,
                   "include": "report_occurrence,report_type,created_by,latest_report"}
        if state:
            params["fields[state]"] = state
        resp = requests.get(f"{API_BASE}/api/v1/report_schedules", params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        page_items = data.get("data", [])
        if not page_items:
            break
        items.extend(page_items)

        oldest_created_raw = page_items[-1].get("attributes", {}).get("created_at")
        if oldest_created_raw:
            oldest_created = datetime.fromisoformat(oldest_created_raw.replace("Z", "+00:00"))
            if oldest_created < stop_before:
                break

        if not data.get("meta", {}).get("next_page"):
            break
    return items


def latest_reports(items, window_start, window_end):
    """Per period (from_date, end_date), the newest B2C report created in the window."""
    matches = {}
    for item in items:
        a = item.get("attributes", {})
        filename = a.get("filename") or ""
        if not (filename.startswith("B2C_Order_Report_") and filename.endswith(".csv")):
            continue
        created_raw = a.get("created_at")
        if not created_raw or not a.get("report_url"):
            continue
        created = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
        if not (window_start <= created <= window_end):
            continue
        key = (a.get("from_date"), a.get("end_date"))
        if key not in matches or created > matches[key]["created"]:
            matches[key] = {
                "filename": filename,
                "report_url": a.get("report_url"),
                "from_date": a.get("from_date"),
                "end_date": a.get("end_date"),
                "created": created,
            }
    return list(matches.values())


def fetch_completed_fallback(jwt):
    """Old behaviour: whatever is currently completed. Used only if nothing
    was found in the target window, so the pipeline still produces
    something rather than failing outright."""
    headers = {
        "Authorization": f"Bearer {jwt}",
        "warehouse": WAREHOUSE,
        "x-language": "en",
        "accept": "application/json",
    }
    resp = requests.get(
        f"{API_BASE}/api/v1/report_schedules",
        params={"page[size]": PAGE_SIZE, "include": "report_occurrence,report_type,created_by,latest_report"},
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    reports = []
    for item in data.get("data", []):
        a = item.get("attributes", {})
        filename = a.get("filename") or ""
        state = a.get("state") or a.get("status")
        if state == "completed" and filename.startswith("B2C_Order_Report_") and filename.endswith(".csv"):
            reports.append({
                "filename": filename,
                "report_url": a.get("report_url"),
                "from_date": a.get("from_date"),
                "end_date": a.get("end_date"),
            })
    return reports


def main():
    env = load_env()
    jwt = login(env["ANCHANTO_EMAIL"], env["ANCHANTO_PASSWORD"])
    print("Logged in to Anchanto WMS.")

    window_start, window_end = lookback_window_utc()
    print(f"Looking for reports created in the last {LOOKBACK_HOURS:g} h "
          f"(UTC {window_start.isoformat()} .. {window_end.isoformat()})")

    all_items = fetch_reports(jwt, None, window_start) + fetch_reports(jwt, "deleted", window_start)
    reports = latest_reports(all_items, window_start, window_end)

    if not reports:
        print("No report was regenerated in that window - falling back to whatever is currently completed.")
        reports = fetch_completed_fallback(jwt)

    if not reports:
        sys.exit("No B2C Order Reports available at all (neither in-window nor currently completed).")

    print(f"\nDownloading {len(reports)} report(s):")
    for r in reports:
        made = f"  created {r['created'].astimezone(WIB):%m-%d %H:%M} WIB" if r.get("created") else ""
        print(f"  {r['filename']}  ({r['from_date']} .. {r['end_date']}){made}")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    for r in reports:
        content = requests.get(r["report_url"], timeout=300).content
        # Anchanto gives every report the same generic filename regardless
        # of period, so multiple reports in one run would otherwise
        # silently overwrite each other in GCS - disambiguate by date range.
        base, ext = os.path.splitext(r["filename"])
        unique_name = f"{base}_{r['from_date']}_{r['end_date']}{ext}"
        dest = f"{GCS_RAW_PREFIX}/{unique_name}"
        bucket.blob(dest).upload_from_string(content, content_type="text/csv")
        print(f"Uploaded {unique_name} ({len(content):,} bytes) -> gs://{GCS_BUCKET}/{dest}")

    print("\nDone.")


if __name__ == "__main__":
    main()
