#!/usr/bin/env python3
"""Read-only diagnostic: print report_schedules entries (non-sensitive
fields only) to see how 'deleted' reports appear via the API, so the
download script can target the right time-window report instead of
whatever happens to be 'completed' when a delayed run finally executes.
"""
import os
import sys

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
API_BASE = "https://scrwms-api.anchanto.com"
WAREHOUSE = "SCR"


def load_env(path=ENV_PATH):
    env = {}
    with open(path, "r", encoding="utf-8-sig") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def login(email, password):
    resp = requests.post(
        f"{API_BASE}/api/login",
        json={"api_user": {"email": email, "password": password}},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["jwt"]


def main():
    env = load_env()
    jwt = login(env["ANCHANTO_EMAIL"], env["ANCHANTO_PASSWORD"])
    headers = {
        "Authorization": f"Bearer {jwt}",
        "warehouse": WAREHOUSE,
        "x-language": "en",
        "accept": "application/json",
    }
    resp = requests.get(
        f"{API_BASE}/api/v1/report_schedules",
        params={"page[size]": 100, "include": "report_occurrence,report_type,created_by,latest_report"},
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    items = data.get("data", [])
    print(f"Total returned: {len(items)} (meta: {data.get('meta')})\n")
    for item in items:
        a = item.get("attributes", {})
        print(
            f"id={item.get('id')} state={a.get('state')} status={a.get('status')} "
            f"deleted_at={a.get('deleted_at')} created_at={a.get('created_at')} "
            f"updated_at={a.get('updated_at')} from={a.get('from_date')} to={a.get('end_date')} "
            f"filename={a.get('filename')}"
        )


if __name__ == "__main__":
    main()
