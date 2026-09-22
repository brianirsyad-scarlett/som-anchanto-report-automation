"""
COMBINED ANCHANTO PROCESSING SCRIPT (GCS INPUT/OUTPUT)
============================================================
- Reads B2C_Order_Report_*.csv from GCS
- Removes rows where Marketplace contains 'INT'
- Validates remaining rows against allowed marketplace list
- Skips rows with empty Marketplace (with warning)
- Handles duplicate output filenames by adding a counter
- Converts to Excel, then to CSV, then to Parquet directly in GCS
"""

import io
import os
import csv
import tempfile
import numpy as np
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from collections import defaultdict
from google.cloud import storage

# Optional speed optimizations
try:
    import polars as pl
    USE_POLARS = True
    print("✓ Polars detected – using high-speed DataFrame operations")
except ImportError:
    USE_POLARS = False
    print("⚠️ Polars not installed – using pandas (slower). Install with: pip install polars")

try:
    import pyarrow
    HAVE_PYARROW = True
except ImportError:
    HAVE_PYARROW = False
    print("⚠️ PyArrow not installed – Parquet compression will be basic. Install with: pip install pyarrow")

# =============================================================================
# CONFIGURATION – EDIT GCS PATHS & SETTINGS AS NEEDED
# =============================================================================

# Bucket configuration
GCS_BUCKET_NAME = "bucket_som"

# Path prefixes inside GCS (without leading or trailing slashes)
B2C_INPUT_PREFIX = "sales_parquet/raw/primary/anchanto/source/raw"
EXCEL_OUTPUT_PREFIX = "sales_parquet/raw/primary/anchanto/source"
CSV_OUTPUT_PREFIX = "sales_parquet/raw/primary/anchanto"

# Exact GCS Blob Keys for Master Data and Final Parquet
MASTER_CSV_BLOB = "sales_parquet/Master Data Product.csv"
OUTPUT_PARQUET_BLOB = "sales_parquet/Anchanto.parquet"

MAX_WORKERS = 10
DELETE_SOURCE = True

# Allowed values for the Marketplace column
ALLOWED_MARKETPLACE_VALUES = {
    "MPL_Shopee Mall",
    "MPL_Tiktok Shop",
    "MPL_Lazada",
    "MPL_Shopee Star",
    "END_Nexus",
    "WNJ_Manual Jatis",
    "WNJ_OfficialWebsite",
    "MPL_Zalora"
}

EXPECTED_COLUMNS = [
    "Marketplace", "Order Date", "Order Number", "Item Upc", "Item Name",
    "Ordered Quantity", "Order Status", "Customer Name", "Shipping City",
    "Shipping Postcode", "Order Packing Date", "Delivery Date (DD/MM/YYYY)",
    "Dispatch Scheduled Date", "Unit Price", "Dispatch Date", "Discount Value"
]

# =============================================================================
# GCS HELPER FUNCTIONS
# =============================================================================

def parse_gcs_blob_name(blob_name):
    """Extract filename from full blob key."""
    return blob_name.split("/")[-1]

def list_gcs_blobs(bucket, prefix, pattern_prefix="B2C_Order_Report_"):
    """List blobs in a GCS folder matching a prefix pattern."""
    blobs = bucket.list_blobs(prefix=prefix)
    matched_blobs = []
    for blob in blobs:
        filename = parse_gcs_blob_name(blob.name)
        if filename.startswith(pattern_prefix) and filename.endswith(".csv"):
            matched_blobs.append(blob)
    return matched_blobs

# =============================================================================
# STEP 1 FUNCTIONS (GCS CSV Input, multiline handling, INT filter, validation)
# =============================================================================

def get_datetime_from_cell(value):
    """Convert any cell value to a Python datetime object."""
    if hasattr(value, 'year') and hasattr(value, 'month') and hasattr(value, 'day'):
        if hasattr(value, 'to_pydatetime'):
            return value.to_pydatetime()
        return value
    if isinstance(value, str):
        try:
            from dateutil import parser
            return parser.parse(value, dayfirst=True)
        except ImportError:
            return pd.to_datetime(value, dayfirst=True).to_pydatetime()
        except Exception:
            pass
    if isinstance(value, (int, float)):
        return pd.to_datetime(value, unit='D', origin='1899-12-30').to_pydatetime()
    raise ValueError(f"Cannot parse date from {value}")

def get_order_date_from_df(df):
    """Extract the first non-null date from the 'Order Date' column."""
    col_name = None
    for col in df.columns:
        if col.lower() == "order date":
            col_name = col
            break
    if col_name is None:
        raise ValueError("Column 'Order Date' not found")
    if USE_POLARS and isinstance(df, pl.DataFrame):
        series = df[col_name]
        first = series.drop_nulls().head(1)
        if len(first) == 0:
            raise ValueError("Order Date column has no valid dates")
        first_val = first.item()
    else:
        series = df[col_name]
        first_val = series.dropna().iloc[0]
    return get_datetime_from_cell(first_val)

def generate_output_filename(order_date, duplicate_counter=0):
    """Generate formatted output filename based on order date."""
    year = order_date.year % 100
    month = order_date.month
    month_abbr = order_date.strftime("%b")
    day = order_date.day
    if day <= 10:
        bracket = "(1)"
    elif day <= 20:
        bracket = "(2)"
    else:
        bracket = "(3)"
    base = f"Anchanto {year:02d} {month:02d} {month_abbr} {bracket} - Order Report"
    if duplicate_counter > 0:
        base += f"_{duplicate_counter}"
    return base + ".xlsx"

def read_gcs_csv_robust(blob):
    """Read a CSV file directly from GCS that may contain multiline quoted fields."""
    content = blob.download_as_bytes().decode('utf-8-sig')
    lines = content.splitlines()

    def logical_lines_generator():
        in_quotes = False
        current_parts = []
        for raw_line in lines:
            line = raw_line.rstrip('\n\r')
            current_parts.append(line)
            quote_count = line.count('"')
            if quote_count % 2 == 1:
                in_quotes = not in_quotes
            if not in_quotes and current_parts:
                yield ' '.join(current_parts)
                current_parts = []
        if current_parts:
            yield ' '.join(current_parts)

    all_rows = []
    for logical_line in logical_lines_generator():
        reader = csv.reader([logical_line], quotechar='"', delimiter=',')
        for row in reader:
            all_rows.append(row)
    if not all_rows:
        raise ValueError("No data rows found in CSV")
    header = all_rows[0]
    if len(header) < len(EXPECTED_COLUMNS):
        raise ValueError(f"CSV header has {len(header)} columns, expected at least {len(EXPECTED_COLUMNS)}")
    data = all_rows[1:]
    df = pd.DataFrame(data, columns=header[:len(header)])
    return df

def filter_and_validate(df):
    """Filter 'INT' and empty Marketplace, validate values."""
    original_count = len(df)
    if USE_POLARS and isinstance(df, pl.DataFrame):
        int_mask = df['Marketplace'].cast(pl.Utf8).str.contains('(?i)INT', literal=False)
        filtered = df.filter(~int_mask)
        removed_int = original_count - filtered.height
        df = filtered.to_pandas()
    else:
        int_mask = df['Marketplace'].astype(str).str.contains('INT', case=False, na=False)
        filtered = df[~int_mask]
        removed_int = original_count - len(filtered)
        df = filtered

    before_empty = len(df)
    df['Marketplace_clean'] = df['Marketplace'].astype(str).str.strip()
    df = df[df['Marketplace_clean'] != '']
    removed_empty = before_empty - len(df)

    invalid_mask = ~df['Marketplace_clean'].isin(ALLOWED_MARKETPLACE_VALUES)
    invalid_rows = df[invalid_mask]
    if not invalid_rows.empty:
        bad_examples = invalid_rows['Marketplace_clean'].dropna().unique()[:10]
        error_msg = (f"Validation failed: {len(invalid_rows)} rows have Marketplace values not in allowed list. "
                     f"Examples: {list(bad_examples)}. This indicates a parsing error.")
        return None, removed_int, removed_empty, error_msg

    df.drop(columns=['Marketplace_clean'], inplace=True)
    return df, removed_int, removed_empty, None

def process_b2c_gcs_blob(blob, bucket, used_filenames_lock, used_filenames):
    """Process a single B2C Order Report CSV blob from GCS."""
    blob_filename = parse_gcs_blob_name(blob.name)
    print(f"\n📄 Processing: {blob_filename}")
    try:
        df = read_gcs_csv_robust(blob)
        print(f"   📖 Read using: robust CSV parser (multiline-aware)")
    except Exception as e:
        return (False, blob, None, f"Failed to read CSV: {e}")

    try:
        order_date = get_order_date_from_df(df)
        print(f"   📅 Order Date: {order_date.strftime('%Y-%m-%d')}")
    except Exception as e:
        return (False, blob, None, f"Failed to parse Order Date: {e}")

    filtered_df, removed_int, removed_empty, validation_err = filter_and_validate(df)
    if validation_err:
        return (False, blob, None, validation_err)

    print(f"   🔍 Removed {removed_int} rows with 'INT' in Marketplace")
    if removed_empty > 0:
        print(f"   ⚠️  Removed {removed_empty} rows with empty Marketplace")
    print(f"   ✅ Kept {len(filtered_df)} valid rows")

    base_filename = generate_output_filename(order_date, 0)
    with used_filenames_lock:
        counter = used_filenames.get(base_filename, 0)
        if counter > 0:
            final_filename = generate_output_filename(order_date, counter)
        else:
            final_filename = base_filename
        used_filenames[base_filename] = counter + 1

    out_excel_blob_key = f"{EXCEL_OUTPUT_PREFIX}/{final_filename}"
    out_blob = bucket.blob(out_excel_blob_key)

    try:
        excel_buffer = io.BytesIO()
        if USE_POLARS and isinstance(filtered_df, pl.DataFrame):
            filtered_df.to_pandas().to_excel(excel_buffer, index=False, engine='openpyxl')
        else:
            filtered_df.to_excel(excel_buffer, index=False, engine='openpyxl')
        excel_buffer.seek(0)
        
        out_blob.upload_from_file(excel_buffer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        print(f"   💾 Saved to GCS: {out_excel_blob_key}")
        return (True, blob, out_blob, f"INT removed: {removed_int}, empty removed: {removed_empty}")
    except Exception as e:
        return (False, blob, None, f"Failed to save Excel: {e}")

# =============================================================================
# STEP 2 FUNCTIONS (GCS Excel to CSV conversion - FIX APPLIED)
# =============================================================================

def excel_to_csv_gcs(excel_blob, bucket):
    try:
        excel_filename = parse_gcs_blob_name(excel_blob.name)
        csv_filename = excel_filename[:-5] + ".csv"
        csv_blob_key = f"{CSV_OUTPUT_PREFIX}/{csv_filename}"
        csv_blob = bucket.blob(csv_blob_key)

        # Check if destination CSV exists
        if not csv_blob.exists():
            should_convert = True
            reason = "CSV created (new file)"
        else:
            # Reload metadata to accurately populate csv_blob.updated timestamp
            csv_blob.reload()
            
            # Compare modification timestamps safely
            if excel_blob.updated and csv_blob.updated and excel_blob.updated > csv_blob.updated:
                should_convert = True
                reason = "CSV updated (Excel was newer)"
            else:
                should_convert = False
                reason = "CSV skipped (already up to date)"

        if should_convert:
            excel_bytes = excel_blob.download_as_bytes()
            df = pd.read_excel(io.BytesIO(excel_bytes), engine='openpyxl')
            
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
            return (True, reason, True, csv_filename)
        else:
            return (True, reason, False, csv_filename)

    except Exception as e:
        return (False, f"Error: {e}", False, None)

# =============================================================================
# STEP 3 FUNCTIONS (Parquet creation from GCS CSVs)
# =============================================================================

def find_column(df, keywords):
    for col in df.columns:
        col_lower = col.lower()
        if all(kw.lower() in col_lower for kw in keywords):
            return col
    return None

FINAL_COLUMN_ORDER = [
    "Source", "Marketplace", "CreatedOn", "SentOn", "Order Number", "Item Upc",
    "Item Name", "Order Status", "Customer Name", "Shipping City", "Shipping Postcode",
    "Ordered Quantity", "Unit Price", "Discount Value", "Brand", "Category",
    "Sub Category", "Variant", "Product Name", "Type of Item",
]

# Columns with a non-string target type in the fixed output schema below.
# Everything else in FINAL_COLUMN_ORDER is a string column.
_INT_COLUMNS = {"Ordered Quantity", "Unit Price", "Discount Value"}
_TIMESTAMP_COLUMNS = {"CreatedOn", "SentOn"}


def build_output_schema():
    """A fixed pyarrow schema every per-file chunk is cast to, so writing
    row groups one file at a time to the same ParquetWriter never hits a
    schema mismatch from one chunk happening to be all-null where another
    isn't."""
    import pyarrow as pa
    fields = []
    for col in FINAL_COLUMN_ORDER:
        if col in _INT_COLUMNS:
            fields.append(pa.field(col, pa.int64()))
        elif col in _TIMESTAMP_COLUMNS:
            fields.append(pa.field(col, pa.timestamp("us")))
        else:
            fields.append(pa.field(col, pa.string()))
    return pa.schema(fields)


def load_product_master(bucket):
    print("\nLoading product master from CSV...")
    master_blob = bucket.blob(MASTER_CSV_BLOB)
    if not master_blob.exists():
        print(f"ERROR: Master file not found at gs://{GCS_BUCKET_NAME}/{MASTER_CSV_BLOB}")
        return None

    master_bytes = master_blob.download_as_bytes()
    # This master file is semicolon-delimited and cp1252-encoded (matches
    # the source Excel export), not comma/utf-8 - a pre-existing mismatch in
    # this script that never surfaced before because earlier runs always
    # crashed on the full-history concat well before reaching this step.
    product_df = pd.read_csv(io.BytesIO(master_bytes), header=0, sep=';', encoding='cp1252')

    item_col = find_column(product_df, ["item", "name"])
    brand_col = find_column(product_df, ["brand"])
    cat_col = find_column(product_df, ["category"])
    subcat_col = find_column(product_df, ["sub", "category"]) or find_column(product_df, ["subcategory"])
    variant_col = find_column(product_df, ["variant"])
    product_name_col = find_column(product_df, ["product", "name"])
    type_col = find_column(product_df, ["type"])

    rename_map = {}
    if item_col: rename_map[item_col] = "ItemName"
    if brand_col: rename_map[brand_col] = "Brand"
    if cat_col: rename_map[cat_col] = "Category"
    if subcat_col: rename_map[subcat_col] = "Sub Category"
    if variant_col: rename_map[variant_col] = "Variant"
    if product_name_col: rename_map[product_name_col] = "Product Name"
    if type_col: rename_map[type_col] = "Type of Item"

    product_master = product_df[list(rename_map.keys())].rename(columns=rename_map)
    product_master["ItemName"] = product_master["ItemName"].astype(str).str.strip().str.upper()
    product_master = product_master.drop_duplicates(subset=["ItemName"])
    product_master = product_master[product_master["ItemName"] != ""]
    print(f"Product master cleaned: {len(product_master):,} unique items")
    return product_master


def transform_chunk(df, product_master):
    """Apply the same per-row transforms the old full-concat version applied
    once to everything, but to a single file's rows at a time."""
    delivery = df.get("Delivery Date (DD/MM/YYYY)", pd.Series([None] * len(df)))
    dispatch = df.get("Dispatch Date", pd.Series([None] * len(df)))
    scheduled = df.get("Dispatch Scheduled Date", pd.Series([None] * len(df)))
    df["SentOn"] = np.where(delivery.notna() & (delivery != ""), delivery,
                             np.where(dispatch.notna() & (dispatch != ""), dispatch, scheduled))

    drop_cols = ["Order Packing Date", "Delivery Date (DD/MM/YYYY)", "Dispatch Scheduled Date", "Dispatch Date"]
    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)
    df.rename(columns={"Order Date": "CreatedOn", "Name": "Source"}, inplace=True)

    for col in ["CreatedOn", "SentOn"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    for col in ["Ordered Quantity", "Unit Price", "Discount Value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')

    text_cols = ["Marketplace", "Order Number", "Item Upc", "Item Name", "Order Status",
                 "Customer Name", "Shipping City", "Shipping Postcode", "Source"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    uppercase_cols = ["Item Name", "Marketplace", "Source", "Order Status", "Customer Name", "Shipping City"]
    for col in uppercase_cols:
        if col in df.columns:
            df[col] = df[col].str.upper()

    df = df.merge(product_master, left_on="Item Name", right_on="ItemName", how="left")
    df.drop(columns=["ItemName"], inplace=True)

    # Guarantee every output column exists (even if this particular file's
    # rows are missing one), so every chunk matches the fixed schema exactly.
    for col in FINAL_COLUMN_ORDER:
        if col not in df.columns:
            df[col] = None
    return df[FINAL_COLUMN_ORDER]


def build_parquet_from_gcs_csvs(bucket):
    print("\n" + "🔹" * 35)
    print("STEP 3: Building Parquet file from GCS CSV files (streamed, one file at a time)")
    print("🔹" * 35)

    if not HAVE_PYARROW:
        print("ERROR: pyarrow is required for the streaming Parquet build.")
        return False
    import pyarrow as pa
    import pyarrow.parquet as pq

    all_csv_blobs = bucket.list_blobs(prefix=f"{CSV_OUTPUT_PREFIX}/")
    csv_blobs = [b for b in all_csv_blobs if b.name.endswith('.csv') and "$" not in parse_gcs_blob_name(b.name)]

    if not csv_blobs:
        print(f"⚠️  No CSV files found in gs://{GCS_BUCKET_NAME}/{CSV_OUTPUT_PREFIX}/")
        return False
    print(f"Found {len(csv_blobs)} CSV file(s).")

    product_master = load_product_master(bucket)
    if product_master is None:
        return False

    schema = build_output_schema()
    total_rows = 0
    out_parquet_blob = bucket.blob(OUTPUT_PARQUET_BLOB)

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
    os.close(tmp_fd)
    try:
        writer = pq.ParquetWriter(tmp_path, schema, compression="zstd")
        try:
            for b in csv_blobs:
                try:
                    content = b.download_as_bytes()
                    df = pd.read_csv(
                        io.BytesIO(content), encoding='utf-8',
                        usecols=lambda col: col in EXPECTED_COLUMNS, dtype=str, low_memory=False,
                    )
                except Exception as e:
                    print(f"Error reading {b.name}: {e}")
                    continue

                filename_without_ext = parse_gcs_blob_name(b.name)[:-4]
                df["Name"] = filename_without_ext
                df = transform_chunk(df, product_master)

                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
                writer.write_table(table)
                total_rows += len(df)
                print(f"Processed {filename_without_ext}: {len(df):,} rows (running total {total_rows:,})")
                del df, table
        finally:
            writer.close()

        if total_rows == 0:
            print("⚠️  No rows written - nothing to upload.")
            return False

        print("\nUploading Parquet to GCS...")
        out_parquet_blob.upload_from_filename(tmp_path, content_type="application/octet-stream")
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass

    out_parquet_blob.reload()
    file_size_gb = out_parquet_blob.size / (1024**3)
    print(f"\n✅ SUCCESS: Parquet saved to gs://{GCS_BUCKET_NAME}/{OUTPUT_PARQUET_BLOB}")
    print(f"   Total rows: {total_rows:,}")
    print(f"   File size: {file_size_gb:.2f} GB")
    return True

# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================

def main():
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    print("=" * 70)
    print(" COMBINED ANCHANTO PROCESSING SCRIPT (GCS INPUT/OUTPUT)")
    print("=" * 70)
    print(f"📁 GCS Bucket:               gs://{GCS_BUCKET_NAME}")
    print(f"📁 B2C Input Prefix:         gs://{GCS_BUCKET_NAME}/{B2C_INPUT_PREFIX}")
    print(f"📁 Excel Output Prefix:      gs://{GCS_BUCKET_NAME}/{EXCEL_OUTPUT_PREFIX}")
    print(f"📁 CSV Output Prefix:        gs://{GCS_BUCKET_NAME}/{CSV_OUTPUT_PREFIX}")
    print(f"📁 Master Data CSV:          gs://{GCS_BUCKET_NAME}/{MASTER_CSV_BLOB}")
    print(f"📁 Output Parquet:           gs://{GCS_BUCKET_NAME}/{OUTPUT_PARQUET_BLOB}")
    print(f"⚙️  Parallel Workers:         {MAX_WORKERS}")
    print(f"🗑️  Delete Source .csv Files: {DELETE_SOURCE}")
    print("=" * 70)

    # STEP 1
    print("\n" + "🔹" * 35)
    print("STEP 1: Processing B2C_Order_Report CSV files in GCS")
    print("🔹" * 35)
    
    b2c_blobs = list_gcs_blobs(bucket, B2C_INPUT_PREFIX)
    if not b2c_blobs:
        print(f"\n⚠️  No B2C_Order_Report .csv files found in prefix: {B2C_INPUT_PREFIX}")
    else:
        print(f"\n📊 Found {len(b2c_blobs)} B2C CSV file(s) to process:")
        for b in b2c_blobs:
            print(f"   • {parse_gcs_blob_name(b.name)}")

        from threading import Lock
        used_filenames_lock = Lock()
        used_filenames = defaultdict(int)

        processed = 0
        failed = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_b2c_gcs_blob, b, bucket, used_filenames_lock, used_filenames): b for b in b2c_blobs}
            for future in tqdm(as_completed(futures), total=len(b2c_blobs), desc="Processing B2C files", unit="file"):
                success, in_blob, out_blob, msg = future.result()
                in_name = parse_gcs_blob_name(in_blob.name)
                if success:
                    processed += 1
                    out_name = parse_gcs_blob_name(out_blob.name)
                    print(f"\n✅ SUCCESS: {in_name}")
                    print(f"   • {msg}")
                    print(f"   • Output: {out_name}")
                    if DELETE_SOURCE:
                        try:
                            in_blob.delete()
                            print(f"   • 🗑️  Deleted original blob: {in_name}")
                        except Exception as e:
                            print(f"   • ⚠️  Failed to delete original blob: {e}")
                else:
                    failed += 1
                    print(f"\n❌ FAILED: {in_name}")
                    print(f"   • Error: {msg}")

        print("\n" + "-" * 70)
        print(f"STEP 1 COMPLETE:")
        print(f"   ✅ Successfully processed: {processed} file(s)")
        print(f"   ❌ Failed: {failed} file(s)")

    # STEP 2
    print("\n" + "🔹" * 35)
    print("STEP 2: Converting Excel files to CSV in GCS")
    print("🔹" * 35)
    
    excel_blobs = [b for b in bucket.list_blobs(prefix=f"{EXCEL_OUTPUT_PREFIX}/") if b.name.endswith('.xlsx')]
    if not excel_blobs:
        print(f"\n⚠️  No Excel files found in prefix: {EXCEL_OUTPUT_PREFIX}")
    else:
        print(f"\n📊 Found {len(excel_blobs)} Excel file(s) to convert:")
        converted = 0
        skipped = 0
        failed = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(excel_to_csv_gcs, xl, bucket): xl for xl in excel_blobs}
            for future in tqdm(as_completed(futures), total=len(excel_blobs), desc="Converting to CSV", unit="file"):
                xl_blob = futures[future]
                xl_name = parse_gcs_blob_name(xl_blob.name)
                success, msg, conv_flag, csv_name = future.result()
                if success:
                    if conv_flag:
                        converted += 1
                        print(f"\n🔄 CONVERTED: {xl_name} → {csv_name}")
                        print(f"   • {msg}")
                    else:
                        skipped += 1
                        print(f"\n⏭️  SKIPPED: {xl_name} → {csv_name}")
                        print(f"   • {msg}")
                else:
                    failed += 1
                    print(f"\n❌ FAILED: {xl_name}")
                    print(f"   • Error: {msg}")

        print("\n" + "-" * 70)
        print(f"STEP 2 COMPLETE:")
        print(f"   🔄 Converted: {converted} file(s)")
        print(f"   ⏭️  Skipped (up to date): {skipped} file(s)")
        print(f"   ❌ Failed: {failed} file(s)")

    # STEP 3
    parquet_ok = build_parquet_from_gcs_csvs(bucket)

    print("\n" + "=" * 70)
    print(" FINAL SUMMARY")
    print("=" * 70)
    print("✅ INT filter applied – rows with 'INT' in Marketplace are REMOVED")
    print(f"✅ Filtered Excel files are in: gs://{GCS_BUCKET_NAME}/{EXCEL_OUTPUT_PREFIX}/")
    print(f"✅ CSV files are in: gs://{GCS_BUCKET_NAME}/{CSV_OUTPUT_PREFIX}/")
    if parquet_ok:
        print(f"✅ Parquet file created: gs://{GCS_BUCKET_NAME}/{OUTPUT_PARQUET_BLOB}")
    else:
        print("⚠️  Parquet file NOT created (no CSV data or missing master file)")
    if DELETE_SOURCE:
        print("✅ Original B2C .csv blobs have been DELETED from GCS")
    else:
        print("ℹ️  Original B2C .csv blobs were preserved (DELETE_SOURCE = False)")

if __name__ == "__main__":
    main()