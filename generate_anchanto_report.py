#!/usr/bin/env python3
"""Generate the B2C Order Report for the CURRENT 10-day period on Anchanto WMS.

The iMac (imac/scrwms-automation/run_refresh.sh) generates this report every 2 hours, at about
:35 past EVEN hours. This script runs in GitHub at :30 past ODD hours (01-21 WIB), so together
the report is regenerated every hour. Like the iMac it first deletes the existing B2C reports
(Anchanto needs that before a new one is made), then creates the new one and waits until its
download link is ready, so the download step right after it picks it up.

Guard - never delete a report another generator is still making: if any B2C report has no
download link yet and is less than MAX_IN_PROGRESS_MIN old, wait up to WAIT_MIN for it; if it
is still in progress, skip this run (delete nothing, create nothing). The download step then
uses the newest finished report, which may be in the Deleted tab.

Exit code 0 also when skipped; non-zero only on an API error or a generated report that did
not finish in time (the workflow mails that).

Credentials: ANCHANTO_EMAIL / ANCHANTO_PASSWORD from .env (written from the repo secrets).
"""
import datetime as dt
import json
import os
import sys
import time

import requests

import download_anchanto_reports as dl  # load_env, login, API_BASE, WAREHOUSE

WIB = dt.timezone(dt.timedelta(hours=7))
PROFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "b2c_order_report.json")
REPORT_TYPE_ID = "3"            # B2C Order Report
MAX_IN_PROGRESS_MIN = 60        # an in-progress report older than this is treated as dead
WAIT_MIN = 15                   # how long to wait for someone else's in-progress report
READY_MAX_MIN = 30              # how long to wait for our own report's download link
POLL_SEC = 15


def headers(jwt, body=False):
    h = {"Authorization": f"Bearer {jwt}", "warehouse": dl.WAREHOUSE, "x-language": "en",
         "accept": "application/json"}
    if body:
        h["Content-Type"] = "application/json"
    return h


def current_segment(today):
    """The fixed third of the month containing `today`: 1-10, 11-20, 21-end."""
    if today.day <= 10:
        return today.replace(day=1), today.replace(day=10)
    if today.day <= 20:
        return today.replace(day=11), today.replace(day=20)
    nxt = (today.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return today.replace(day=21), nxt - dt.timedelta(days=1)


def b2c_schedules(jwt):
    """Every B2C Order Report schedule on the home list (all pages): (id, attributes)."""
    out, page = [], 1
    while True:
        r = requests.get(f"{dl.API_BASE}/api/v1/report_schedules",
                         params={"page[number]": page, "page[size]": 100, "include": "report_type"},
                         headers=headers(jwt), timeout=60)
        r.raise_for_status()
        data = r.json()
        rows = data.get("data", [])
        for row in rows:
            tid = (((row.get("relationships") or {}).get("report_type") or {}).get("data") or {}).get("id")
            if str(tid) == REPORT_TYPE_ID:
                out.append((int(row["id"]), row.get("attributes") or {}))
        nxt = (data.get("meta") or {}).get("next_page")
        if not rows or not nxt:
            return out
        page = nxt


def in_progress(rows, now):
    """Rows still generating (no link, not failed) and young enough to be alive."""
    busy = []
    for sid, a in rows:
        state = str(a.get("state") or "").lower()
        status = str(a.get("status") or "").lower()
        if a.get("report_url") or state in {"failed", "error"} or status in {"failed", "error"}:
            continue
        created = a.get("created_at")
        if created:
            age = now - dt.datetime.fromisoformat(created.replace("Z", "+00:00"))
            if age > dt.timedelta(minutes=MAX_IN_PROGRESS_MIN):
                continue
        busy.append(sid)
    return busy


def main():
    env = dl.load_env()
    jwt = dl.login(env["ANCHANTO_EMAIL"], env["ANCHANTO_PASSWORD"])
    print("Logged in to Anchanto WMS.")

    # 1. Guard: someone else (the iMac) still generating?
    waited = 0
    while True:
        rows = b2c_schedules(jwt)
        busy = in_progress(rows, dt.datetime.now(dt.timezone.utc))
        if not busy:
            break
        if waited >= WAIT_MIN * 60:
            print(f"SKIP: B2C report(s) {busy} still generating after {WAIT_MIN} min - not deleting "
                  "another generator's report. The download step uses the newest finished one.")
            return 0
        print(f"  waiting: B2C report(s) {busy} still generating ({waited // 60} min)")
        time.sleep(60)
        waited += 60

    # 2. Delete the existing B2C reports (Anchanto needs this before a new one; finished reports
    #    move to the Deleted tab and keep their download links).
    ids = sorted(sid for sid, _ in rows)
    if ids:
        r = requests.delete(f"{dl.API_BASE}/api/v1/report_schedules/bulk_delete",
                            json={"ids": ids, "filter": {}}, headers=headers(jwt, body=True), timeout=60)
        r.raise_for_status()
        print(f"Deleted {len(ids)} existing B2C report(s): {ids}")
    else:
        print("No existing B2C report to delete.")

    # 3. Create the current period's report.
    start, end = current_segment(dt.datetime.now(WIB).date())
    with open(PROFILE, encoding="utf-8") as fh:
        sched = dict(json.load(fh)["report_schedule"])
    sched["from_date"] = start.strftime("%a %b %d %Y")   # the web app sends JS toDateString()
    sched["end_date"] = end.strftime("%a %b %d %Y")
    r = requests.post(f"{dl.API_BASE}/api/v1/report_schedules", json={"report_schedule": sched},
                      headers=headers(jwt, body=True), timeout=60)
    r.raise_for_status()
    sid = int(r.json()["data"]["id"])
    print(f"Created B2C report id={sid} for {start} .. {end}")

    # 4. Wait for its download link, so the download step finds it.
    deadline = time.time() + READY_MAX_MIN * 60
    while time.time() < deadline:
        for rid, a in b2c_schedules(jwt):
            if rid != sid:
                continue
            if a.get("report_url"):
                print(f"Report id={sid} ready.")
                return 0
            if str(a.get("state") or "").lower() in {"failed", "error"}:
                sys.exit(f"Report id={sid} failed on Anchanto: {json.dumps(a)[:300]}")
        time.sleep(POLL_SEC)
    sys.exit(f"Report id={sid} not ready after {READY_MAX_MIN} min (it will still be emailed).")


if __name__ == "__main__":
    sys.exit(main())
