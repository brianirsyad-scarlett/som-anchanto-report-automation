#!/usr/bin/env bash
# Decide whether this run should do its work.  Prints run=true|false to
# $GITHUB_OUTPUT.
#
#     guard.sh <workflow file> day        once per WIB day
#     guard.sh <workflow file> <minutes>  once per that many minutes
#
# Runs started by hand or by dispatch.yml (event workflow_dispatch) always
# run. A backup run (schedule, or workflow_run chaining) is skipped when
# another run of the same workflow already succeeded - or is still running -
# inside the window. GitHub's cron is often hours late, so without this the
# backup would repeat work the on-time run already did.
set -euo pipefail
wf="$1"; window="$2"

if [ "${GITHUB_EVENT_NAME}" = "workflow_dispatch" ]; then
  echo "run=true" >> "$GITHUB_OUTPUT"; echo "started on demand - running"; exit 0
fi

if [ "$window" = "day" ]; then
  since=$(date -u -d "$(TZ=Asia/Jakarta date +%F) 00:00 +0700" +%FT%TZ)
else
  since=$(date -u -d "-${window} minutes" +%FT%TZ)
fi

others=$(gh api "repos/${GITHUB_REPOSITORY}/actions/workflows/${wf}/runs?created=%3E%3D${since}&per_page=50" \
  --jq "[.workflow_runs[] | select(.id != ${GITHUB_RUN_ID})
         | select(.conclusion == \"success\" or .status != \"completed\")
         | select(.event != \"schedule\" or .conclusion == \"success\")] | length")

if [ "$others" -gt 0 ]; then
  echo "run=false" >> "$GITHUB_OUTPUT"
  echo "::notice::backup run skipped - ${wf} already ran since ${since} (${others} run(s))"
else
  echo "run=true" >> "$GITHUB_OUTPUT"
  echo "no ${wf} run since ${since} - this backup run does the work"
fi
