# som-anchanto-report-automation

Runs the Anchanto B2C Order Report pipeline on GitHub Actions: downloads
completed reports from Anchanto WMS, uploads the raw CSVs to GCS, then
processes them into Excel, CSV, and a combined Parquet file - all in GCS.

Report *creation* itself is handled by a separate, already-existing
automation elsewhere - this pipeline only checks for reports already marked
"completed" and picks them up. It grabs however many are ready (not a fixed
count), since the number of reports ready at run time can vary.

## Pipeline

1. `download_anchanto_reports.py` - logs into `scrwms-api.anchanto.com`,
   lists report schedules, and uploads every `B2C_Order_Report_*.csv` marked
   `completed` to `gs://bucket_som/sales_parquet/raw/primary/anchanto/source/raw/`.
   Polls for up to 20 minutes if nothing is ready yet.
2. `BigQuery_Anchanto.py` (unchanged copy of the existing local script) -
   reads those raw CSVs from GCS, filters/validates rows, converts to Excel
   (named by the order date found in the file), converts to CSV, then
   combines every CSV into `gs://bucket_som/sales_parquet/Anchanto.parquet`
   (merged with the product master data already mirrored to GCS). Deletes
   the raw source CSVs from GCS after processing.

Reprocessing a report that was already consumed is harmless: since the
output filename is derived from the order date inside the file, re-running
on the same data just overwrites the same output rather than duplicating
rows, and the Parquet step always rebuilds from every CSV currently present.

## Setup

In this repo's Settings -> Secrets and variables -> Actions, add:

- `ANCHANTO_EMAIL`
- `ANCHANTO_PASSWORD`
- `GCP_SA_KEY` - the full contents of the GCS service-account JSON key (the
  same one the other SOM pipelines use)

(Values are never committed to this repo - see `.env.example` for the
format if running locally instead.)

## Schedule

00:30 Asia/Jakarta (WIB, UTC+7) daily (`cron: "30 17 * * *"`, i.e. 17:30 UTC
the previous day). Also runs on demand via `workflow_dispatch`.
