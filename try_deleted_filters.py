#!/usr/bin/env python3
"""Read-only: try a few common query-param patterns for including
soft-deleted report_schedules, to see if any reveal more than the 1
non-deleted report the plain call returns."""
import os

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


CANDIDATE_PARAM_SETS = [
    {"page[size]": 100, "with_deleted": "true"},
    {"page[size]": 100, "filter[state]": "deleted"},
    {"page[size]": 100, "filter[deleted]": "true"},
    {"page[size]": 100, "q[deleted_at_not_null]": "1"},
    {"page[size]": 100, "scope": "deleted"},
    {"page[size]": 100, "tab": "deleted"},
    {"page[size]": 100, "status": "deleted"},
    {"page[size]": 100, "state": "deleted"},
    {"page[size]": 100, "only_deleted": "true"},
    {"page[size]": 100, "include_deleted": "true"},
]


def main():
    env = load_env()
    jwt = login(env["ANCHANTO_EMAIL"], env["ANCHANTO_PASSWORD"])
    headers = {
        "Authorization": f"Bearer {jwt}",
        "warehouse": WAREHOUSE,
        "x-language": "en",
        "accept": "application/json",
    }
    for params in CANDIDATE_PARAM_SETS:
        try:
            resp = requests.get(
                f"{API_BASE}/api/v1/report_schedules",
                params=params,
                headers=headers,
                timeout=30,
            )
            data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else None
            count = len(data.get("data", [])) if data else "n/a"
            total = data.get("meta", {}).get("total_count") if data else "n/a"
            print(f"{params} -> HTTP {resp.status_code}, items={count}, total_count={total}")
        except Exception as e:
            print(f"{params} -> ERROR {e}")


if __name__ == "__main__":
    main()
