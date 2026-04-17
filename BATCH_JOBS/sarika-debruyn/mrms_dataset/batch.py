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
# ]
# ///
 
import argparse
import gzip
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
 
import boto3
import cfgrib
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

# ── GCS output path ────────────────────────────────────────────────────────────
GCS_BUCKET = "leap-persistent"
GCS_PREFIX = "sarika/nyc_flooding/mrms"
OUTPUT_PATH = f"gs://{GCS_BUCKET}/{GCS_PREFIX}" #remove when doing skylink batch jobs

# ── S3 config ──────────────────────────────────────────────────────────────────
S3_BUCKET = "noaa-mrms-pds"
S3_PREFIX = "CONUS/PrecipRate_00.00"
S3_REGION = "us-east-1"

# Thread-safe counter for progress reporting
_progress_lock = Lock()
_progress_count = 0


def make_s3_client():
    """Each thread needs its own S3 client — boto3 clients are not thread-safe."""
    return boto3.client(
        "s3",
        region_name=S3_REGION,
        config=Config(
            signature_version=UNSIGNED,
            max_pool_connections=50,  # allow more concurrent connections
        ),
    )


def list_mrms_keys(date) -> list:
    client = make_s3_client()
    prefix = f"{S3_PREFIX}/{date.strftime('%Y%m%d')}/"
    keys   = []
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


def download_and_parse_one(key: str, total: int):
    """
    Download, decompress, parse, clip one GRIB2 file.
    Returns (timestamp, DataArray) or None on failure.
    Each call creates its own S3 client — safe for threading.
    """
    global _progress_count

    client = make_s3_client()
    try:
        obj = client.get_object(Bucket=S3_BUCKET, Key=key)
        raw = obj["Body"].read()

        if key.endswith(".gz"):
            raw = gzip.decompress(raw)

        # Each thread writes to its own tmp file using thread id
        tid      = threading.get_ident()
        tmp_path = Path(f"/tmp/mrms_tmp_{tid}.grib2")
        tmp_path.write_bytes(raw)

        datasets = cfgrib.open_datasets(str(tmp_path))
        if not datasets:
            return None

        ds = datasets[0]

        precip_var = None
        for candidate in ["paramId_0", "unknown", "PrecipRate", "tp", "var209"]:
            if candidate in ds:
                precip_var = candidate
                break
        if precip_var is None:
            precip_var = list(ds.data_vars)[0]

        da = ds[precip_var]

        # Convert longitudes 0–360 → -180–180
        lons = da.longitude.values.copy()
        lons = np.where(lons > 180, lons - 360, lons)
        da   = da.assign_coords(longitude=("longitude", lons))

        # Clip to NYC
        da = da.where(
            (da.latitude  >= NYC_LAT_MIN) & (da.latitude  <= NYC_LAT_MAX) &
            (da.longitude >= NYC_LON_MIN) & (da.longitude <= NYC_LON_MAX),
            drop=True,
        )

        # Replace no-data sentinels
        da = da.where(da >= 0)

        ts = parse_timestamp_from_key(key)

        # Thread-safe progress logging every 50 files
        with _progress_lock:
            _progress_count += 1
            if _progress_count % 50 == 0:
                log.info(f"  Progress: {_progress_count}/{total} files downloaded")

        return (ts, da)

    except Exception as e:
        log.warning(f"  Failed: {Path(key).name} — {e}")
        return None


def interpolate_to_1min(da: xr.DataArray) -> xr.DataArray:
    t_start   = pd.Timestamp(da.time.values[0])
    t_end     = pd.Timestamp(da.time.values[-1])
    
    # xarray interp doesn't support tz-aware timestamps — use naive UTC
    new_times = pd.date_range(start=t_start, end=t_end, freq="1min").tz_localize(None)
    
    # Strip tz from the DataArray's time coordinate before interpolating
    da = da.assign_coords(time=da.time.values.astype("datetime64[ns]"))
    
    interpolated = da.interp(
        time=new_times,
        method="linear",
        kwargs={"fill_value": np.nan},
    )
    
    return interpolated


def gcs_file_exists(fs, gcs_path: str) -> bool:
    try:
        return fs.exists(gcs_path)
    except Exception:
        return False


def process_one_day(date, fs, n_workers: int) -> bool:
    global _progress_count
    _progress_count = 0

    fname    = f"mrms_nyc_1min_{date.strftime('%Y%m%d')}.csv"
    gcs_path = f"{OUTPUT_PATH}/{fname}"

    # Skip if already written — safe to resume after interruption
    if gcs_file_exists(fs, gcs_path):
        log.info(f"  SKIP {date} — already exists at {gcs_path}")
        return True

    log.info(f"── {date} ─────────────────────────────────────────────")
    t0   = time.time()
    keys = list_mrms_keys(date)
    if not keys:
        log.warning(f"  No S3 files found for {date}")
        return False

    log.info(f"  Downloading {len(keys)} files with {n_workers} workers ...")

    # ── Parallel download + parse ──────────────────────────────────────────────
    results = {}
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(download_and_parse_one, key, len(keys)): key
            for key in keys
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                ts, da = result
                results[ts] = da

    if not results:
        log.warning(f"  All files failed for {date}")
        return False

    log.info(f"  Downloaded {len(results)}/{len(keys)} files in {time.time()-t0:.0f}s")

    # ── Stack ──────────────────────────────────────────────────────────────────
    try:
        log.info(f"  Stacking {len(results)} snapshots ...")
        sorted_das = [
            results[ts].expand_dims(time=[ts])
            for ts in sorted(results.keys())
        ]
        stacked = xr.concat(sorted_das, dim="time", coords="minimal", compat="override")
        del results, sorted_das
    except Exception as e:
        log.error(f"  Stacking failed: {e}")
        return False

    # ── Interpolate ────────────────────────────────────────────────────────────
    try:
        log.info(f"  Interpolating 2-min → 1-min ...")
        interpolated = interpolate_to_1min(stacked)
        del stacked
    except Exception as e:
        log.error(f"  Interpolation failed: {e}")
        return False

    # ── Convert to DataFrame ───────────────────────────────────────────────────
    try:
        log.info(f"  Converting to DataFrame ...")
        df = (
            interpolated.to_dataframe(name="precip_rate_mmhr")
                        .reset_index()
                        [["time", "latitude", "longitude", "precip_rate_mmhr"]]
        )
        del interpolated
        df = df.dropna(subset=["precip_rate_mmhr"])
        log.info(f"  DataFrame shape: {df.shape}")
    except Exception as e:
        log.error(f"  DataFrame conversion failed: {e}")
        return False

    # ── Write to GCS ───────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    log.info(f"  Writing {len(df):,} rows → {gcs_path} ...")
    try:
        with fs.open(gcs_path, "w") as f:
            df.to_csv(f, index=False)
        log.info(f"  Done in {elapsed:.0f}s — {len(df):,} rows saved to GCS")
    except Exception as e:
        log.error(f"  GCS write failed: {e}")
        # Save locally as fallback so you don't lose the data
        local_path = f"/tmp/{fname}"
        try:
            df.to_csv(local_path, index=False)
            log.info(f"  Saved locally as fallback: {local_path}")
        except Exception as e2:
            log.error(f"  Local fallback also failed: {e2}")
        return False

    del df
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Pull MRMS PrecipRate for NYC, clip, interpolate to 1-min, write CSV to GCS."
    )
    parser.add_argument("--start",   required=True, help="Start date YYYY-MM-DD (UTC)")
    parser.add_argument("--end",     required=True, help="End date   YYYY-MM-DD (UTC)")
    parser.add_argument("--workers", type=int, default=16,
                        help="Parallel download threads per day (default: 16, max recommended: 32)")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end   = datetime.strptime(args.end,   "%Y-%m-%d").date()
    date_range = [
        start + timedelta(days=i)
        for i in range((end - start).days + 1)
    ]

    n_days = len(date_range)
    est_min = n_days * 0.33   # ~20s per day with 16 workers
    est_max = n_days * 0.67   # ~40s per day with 16 workers

    log.info(f"Date range   : {start} → {end}  ({n_days} day(s))")
    log.info(f"Workers      : {args.workers} parallel download threads")
    log.info(f"GCS output   : gs://{GCS_BUCKET}/{GCS_PREFIX}/mrms_nyc_1min_YYYYMMDD.csv")
    log.info(f"Est. runtime : {est_min:.0f}–{est_max:.0f} minutes  "
             f"(~20–40s/day × {n_days} days)")
    log.info(f"Resume safe  : yes — completed days are skipped automatically")

    fs = gcsfs.GCSFileSystem()

    try:
        fs.ls(GCS_BUCKET)
        log.info(f"GCS auth OK  : gs://{GCS_BUCKET}/ is accessible")
    except Exception as e:
        log.error(f"GCS access failed: {e}")
        log.error("Try: gcloud auth application-default login")
        return

    succeeded, failed = 0, 0
    total_t0 = time.time()

    for i, date in enumerate(date_range, 1):
        log.info(f"[{i}/{n_days}]")
        ok = process_one_day(date, fs, args.workers)
        if ok:
            succeeded += 1
        else:
            failed += 1

        # ETA update every 10 days
        if i % 10 == 0:
            elapsed  = time.time() - total_t0
            rate     = elapsed / i          # seconds per day
            remaining = rate * (n_days - i)
            log.info(
                f"  ETA: ~{remaining/60:.0f} min remaining  "
                f"({rate:.0f}s/day avg)"
            )

    total_elapsed = time.time() - total_t0
    log.info("─────────────────────────────────────────────────────────")
    log.info(f"Complete : {succeeded} days written, {failed} days failed")
    log.info(f"Total time: {total_elapsed/60:.1f} minutes")
    log.info(f"Output   : gs://{GCS_BUCKET}/{GCS_PREFIX}/")


if __name__ == "__main__":
    main()
