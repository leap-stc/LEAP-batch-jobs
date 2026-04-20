# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "boto3",
#     "cfgrib",
#     "xarray",
#     "gcsfs",
#     "numpy",
#     "pandas",
#     "botocore",
#     "dask[array]",
# ]
# ///

"""
MRMS PrecipRate pipeline
========================
- Pulls MRMS_PrecipRate_00.00 from s3://noaa-mrms-pds (available from ~Oct 2020)
- Clips to NYC bounding box (full 1 km spatial grid preserved, no sensor matching)
- Interpolates 2-min native cadence → 1-min timestamps (linear)
- Writes one CSV per day:  gs://leap-persistent/nyc_flooding/mrms/mrms_nyc_1min_YYYYMMDD.csv
  Columns: time, latitude, longitude, precip_rate_mmhr
- Resume-safe: skips days whose output file already exists in GCS
- Uses Dask for parallel stacking/interpolation of the daily xarray cube

Usage:
    python mrms_aggregation_pipeline.py --start 2020-10-01 --end 2026-04-20
    python mrms_aggregation_pipeline.py --start 2020-10-01 --end 2026-04-20 --workers 32
"""

import argparse
import gzip
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock

import boto3
import cfgrib
import dask
import dask.array as da
import gcsfs
import numpy as np
import pandas as pd
import xarray as xr
from botocore import UNSIGNED
from botocore.config import Config

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── NYC bounding box ───────────────────────────────────────────────────────────
NYC_LAT_MIN = 40.45
NYC_LAT_MAX = 40.95
NYC_LON_MIN = -74.30
NYC_LON_MAX = -73.65

# ── GCS output ─────────────────────────────────────────────────────────────────
GCS_BUCKET = "leap-persistent"
GCS_PREFIX = "nyc_flooding/mrms"
OUTPUT_PATH = f"gs://{GCS_BUCKET}/{GCS_PREFIX}"

# ── S3 source ──────────────────────────────────────────────────────────────────
S3_BUCKET = "noaa-mrms-pds"
S3_PREFIX = "CONUS/PrecipRate_00.00"
S3_REGION = "us-east-1"

# ── Thread-safe download counter ───────────────────────────────────────────────
_progress_lock = Lock()
_progress_count = 0


# ──────────────────────────────────────────────────────────────────────────────
# S3 helpers
# ──────────────────────────────────────────────────────────────────────────────

def make_s3_client():
    """boto3 clients are NOT thread-safe — each thread creates its own."""
    return boto3.client(
        "s3",
        region_name=S3_REGION,
        config=Config(signature_version=UNSIGNED, max_pool_connections=50),
    )


def list_mrms_keys(date) -> list:
    client = make_s3_client()
    prefix = f"{S3_PREFIX}/{date.strftime('%Y%m%d')}/"
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".grib2.gz") or k.endswith(".grib2"):
                keys.append(k)
    return sorted(keys)


def parse_timestamp_from_key(key: str) -> pd.Timestamp:
    fname  = Path(key).name
    dt_str = fname.split("_")[-1].split(".grib2")[0]
    return pd.to_datetime(dt_str, format="%Y%m%d-%H%M%S").tz_localize("UTC")


# ──────────────────────────────────────────────────────────────────────────────
# Per-file download + parse (runs inside thread pool)
# ──────────────────────────────────────────────────────────────────────────────

def download_and_parse_one(key: str, total: int):
    """
    Download → decompress → parse → clip one GRIB2 file.
    Returns (timestamp, DataArray clipped to NYC) or None on failure.
    Thread-safe: each call owns its own S3 client and tmp file.
    """
    global _progress_count
    client = make_s3_client()
    try:
        raw = client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)

        tid      = threading.get_ident()
        tmp_path = Path(f"/tmp/mrms_tmp_{tid}.grib2")
        tmp_path.write_bytes(raw)

        datasets = cfgrib.open_datasets(str(tmp_path))
        if not datasets:
            return None
        ds = datasets[0]

        # Resolve variable name — MRMS GRIB2 uses non-standard parameter IDs
        precip_var = next(
            (v for v in ["paramId_0", "unknown", "PrecipRate", "tp", "var209"]
             if v in ds),
            list(ds.data_vars)[0],
        )
        da_raw = ds[precip_var]

        # Fix longitude convention: MRMS uses 0-360, convert to -180-180
        lons = np.where(da_raw.longitude.values > 180,
                        da_raw.longitude.values - 360,
                        da_raw.longitude.values)
        da_raw = da_raw.assign_coords(longitude=("longitude", lons))

        # Clip to NYC bounding box — keeps full 1 km spatial grid, just smaller
        da_nyc = da_raw.where(
            (da_raw.latitude  >= NYC_LAT_MIN) & (da_raw.latitude  <= NYC_LAT_MAX) &
            (da_raw.longitude >= NYC_LON_MIN) & (da_raw.longitude <= NYC_LON_MAX),
            drop=True,
        )

        # Mask no-data sentinels (MRMS uses -3 / -999)
        da_nyc = da_nyc.where(da_nyc >= 0)

        ts = parse_timestamp_from_key(key)

        with _progress_lock:
            _progress_count += 1
            if _progress_count % 50 == 0:
                pct = 100 * _progress_count / total
                print(f"  [{_progress_count}/{total}  {pct:.0f}%] files downloaded", flush=True)

        return (ts, da_nyc)

    except Exception as exc:
        log.warning(f"  SKIP {Path(key).name} — {exc}")
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Interpolation
# ──────────────────────────────────────────────────────────────────────────────

def interpolate_to_1min(stacked: xr.DataArray) -> xr.DataArray:
    """
    Upsample from native 2-min MRMS cadence to 1-min timestamps via linear
    interpolation. Chunks along time so Dask can parallelise across the
    spatial cube.
    """
    t_start = pd.Timestamp(stacked.time.values[0])
    t_end   = pd.Timestamp(stacked.time.values[-1])
    new_times = pd.date_range(start=t_start, end=t_end, freq="1min")

    # xarray.interp does not support tz-aware timestamps — strip tz first
    stacked_naive = stacked.assign_coords(
        time=stacked.time.values.astype("datetime64[ns]")
    )
    new_times_naive = new_times.tz_localize(None)

    # Chunk along time so Dask parallelises interpolation across lat/lon
    stacked_chunked = stacked_naive.chunk({"time": 30})

    interpolated = stacked_chunked.interp(
        time=new_times_naive,
        method="linear",
        kwargs={"fill_value": np.nan},
    )
    return interpolated


# ──────────────────────────────────────────────────────────────────────────────
# GCS helpers
# ──────────────────────────────────────────────────────────────────────────────

def gcs_file_exists(fs, gcs_path: str) -> bool:
    try:
        return fs.exists(gcs_path)
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Per-day processing
# ──────────────────────────────────────────────────────────────────────────────

def process_one_day(date, fs, n_workers: int) -> bool:
    global _progress_count
    _progress_count = 0

    fname    = f"mrms_nyc_1min_{date.strftime('%Y%m%d')}.csv"
    gcs_path = f"{OUTPUT_PATH}/{fname}"

    if gcs_file_exists(fs, gcs_path):
        print(f"  SKIP {date} — already exists: {gcs_path}", flush=True)
        return True

    print(f"\n{'─'*60}", flush=True)
    print(f"  Processing {date}", flush=True)
    t0 = time.time()

    # ── 1. List S3 keys ───────────────────────────────────────────────────────
    print(f"  Listing S3 keys ...", flush=True)
    keys = list_mrms_keys(date)
    if not keys:
        log.warning(f"  No S3 files found for {date} — skipping")
        return False
    print(f"  Found {len(keys)} GRIB2 files", flush=True)

    # ── 2. Parallel download + parse ──────────────────────────────────────────
    print(f"  Downloading with {n_workers} threads ...", flush=True)
    results = {}
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(download_and_parse_one, key, len(keys)): key
            for key in keys
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                ts, da_nyc = result
                results[ts] = da_nyc

    n_ok = len(results)
    print(f"  Downloaded {n_ok}/{len(keys)} files in {time.time()-t0:.0f}s", flush=True)
    if n_ok == 0:
        log.error(f"  All files failed for {date}")
        return False

    # ── 3. Stack into (time, lat, lon) DataArray ──────────────────────────────
    print(f"  Stacking {n_ok} snapshots ...", flush=True)
    try:
        sorted_das = [
            results[ts].expand_dims(time=[ts])
            for ts in sorted(results.keys())
        ]
        stacked = xr.concat(sorted_das, dim="time", coords="minimal", compat="override")
        del results, sorted_das
        print(
            f"  Stacked shape: {stacked.shape}  "
            f"(time={stacked.sizes['time']}, "
            f"lat={stacked.sizes['latitude']}, "
            f"lon={stacked.sizes['longitude']})",
            flush=True,
        )
    except Exception as exc:
        log.error(f"  Stacking failed: {exc}")
        return False

    # ── 4. Interpolate 2-min → 1-min (Dask-backed) ───────────────────────────
    print(f"  Interpolating to 1-min timestamps (Dask) ...", flush=True)
    try:
        interpolated = interpolate_to_1min(stacked)
        del stacked
        with dask.config.set(scheduler="threads"):
            interpolated = interpolated.compute()
        print(
            f"  Interpolated shape: {interpolated.shape}  "
            f"({interpolated.sizes['time']} timesteps)",
            flush=True,
        )
    except Exception as exc:
        log.error(f"  Interpolation failed: {exc}")
        return False

    # ── 5. Convert to flat DataFrame ──────────────────────────────────────────
    # Full NYC spatial grid — columns: time, latitude, longitude, precip_rate_mmhr
    # Sensor matching is done as a separate downstream step.
    print(f"  Converting to DataFrame ...", flush=True)
    try:
        df = (
            interpolated
            .to_dataframe(name="precip_rate_mmhr")
            .reset_index()
            [["time", "latitude", "longitude", "precip_rate_mmhr"]]
        )
        del interpolated
        df = df.dropna(subset=["precip_rate_mmhr"])
        print(f"  DataFrame shape: {df.shape}  ({len(df):,} rows)", flush=True)
    except Exception as exc:
        log.error(f"  DataFrame conversion failed: {exc}")
        return False

    # ── 6. Write to GCS ───────────────────────────────────────────────────────
    elapsed = time.time() - t0
    print(f"  Writing to GCS: {gcs_path} ...", flush=True)
    try:
        with fs.open(gcs_path, "w") as f:
            df.to_csv(f, index=False)
        print(f"  Done in {elapsed:.0f}s — {len(df):,} rows -> {gcs_path}", flush=True)
    except Exception as exc:
        log.error(f"  GCS write failed: {exc}")
        local_path = f"/tmp/{fname}"
        try:
            df.to_csv(local_path, index=False)
            log.warning(f"  Saved locally as fallback: {local_path}")
        except Exception as exc2:
            log.error(f"  Local fallback also failed: {exc2}")
        return False

    del df
    return True


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Convert MRMS PrecipRate GRIB2 to NYC-clipped 1-min CSV on GCS.\n"
            "Preserves full 1 km spatial grid (no sensor matching).\n"
            "S3 archive starts ~2020-10-01."
        )
    )
    parser.add_argument("--start",   required=True, help="Start date YYYY-MM-DD (UTC)")
    parser.add_argument("--end",     required=True, help="End date   YYYY-MM-DD (UTC)")
    parser.add_argument("--workers", type=int, default=16,
                        help="Parallel S3 download threads per day (default: 16)")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end   = datetime.strptime(args.end,   "%Y-%m-%d").date()
    date_range = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    n_days = len(date_range)

    print("=" * 60, flush=True)
    print(f"  MRMS NYC pipeline", flush=True)
    print(f"  Date range : {start} -> {end}  ({n_days} days)", flush=True)
    print(f"  Workers    : {args.workers} download threads/day", flush=True)
    print(f"  GCS output : gs://{GCS_BUCKET}/{GCS_PREFIX}/mrms_nyc_1min_YYYYMMDD.csv", flush=True)
    print(f"  Spatial    : NYC bbox, full 1 km grid, no sensor matching", flush=True)
    print(f"  Temporal   : 2-min MRMS -> 1-min linear interpolation", flush=True)
    print(f"  Resume     : yes — completed days auto-skipped", flush=True)
    print(f"  Est. time  : {n_days * 0.5:.0f}-{n_days * 1.0:.0f} min  (~30-60s/day)", flush=True)
    print("=" * 60, flush=True)

    fs = gcsfs.GCSFileSystem()
    try:
        fs.ls(GCS_BUCKET)
        print(f"  GCS auth OK: gs://{GCS_BUCKET}/ accessible", flush=True)
    except Exception as exc:
        log.error(f"GCS access failed: {exc}")
        log.error("Run: gcloud auth application-default login")
        return

    succeeded, failed = 0, 0
    total_t0 = time.time()

    for i, date in enumerate(date_range, 1):
        print(f"\n[{i}/{n_days}]", flush=True)
        ok = process_one_day(date, fs, args.workers)
        if ok:
            succeeded += 1
        else:
            failed += 1

        # ETA update every 10 days
        if i % 10 == 0:
            elapsed   = time.time() - total_t0
            rate      = elapsed / i
            remaining = rate * (n_days - i)
            print(
                f"\n  ETA: ~{remaining/60:.0f} min remaining  "
                f"({rate:.0f}s/day avg)  "
                f"[{succeeded} done, {failed} failed]",
                flush=True,
            )

    total_elapsed = time.time() - total_t0
    print("\n" + "=" * 60, flush=True)
    print(f"  COMPLETE", flush=True)
    print(f"  {succeeded} days written, {failed} days failed", flush=True)
    print(f"  Total time : {total_elapsed/60:.1f} minutes", flush=True)
    print(f"  Output     : gs://{GCS_BUCKET}/{GCS_PREFIX}/", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    main()