# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pandas",
#   "numpy",
#   "pyarrow",
#   "gcsfs",
# ]
# ///

"""
batch.py
=========
Joins MRMS precipitation data with FloodNet water depth data.

Steps:
  1. Read and cache FloodNet CSV from GCS
  2. Match each sensor to its nearest MRMS grid point (Haversine)
  3. Loop over MRMS daily files, filter to needed grid points, merge on timestamp
  4. Save merged Parquet to GCS

Outputs written to gs://leap-persistent/nyc_flooding/merged/
"""

import math
import re
import warnings
import numpy as np
import pandas as pd
import gcsfs
from pathlib import Path

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
MRMS_DIR         = "gs://leap-persistent/nyc_flooding/mrms/"
FLOODNET_CSV     = "gs://leap-persistent/nyc_flooding/floodnet/floodnet_depth_all_20260313_094427.csv"
SENSOR_META_CSV  = "gs://leap-persistent/nyc_flooding/PerSensorData.csv"
OUTPUT_DIR       = "gs://leap-persistent/nyc_flooding/merged/"

# Local scratch paths on the VM (fast, ephemeral)
LOCAL_FN_PARQUET = Path("/tmp/floodnet_cached.parquet")
LOCAL_MERGED     = Path("/tmp/merged_mrms_floodnet.parquet")

# Sample MRMS file used to extract grid geometry
GRID_SAMPLE_FILE = "mrms_nyc_1min_20230929.csv"
# ═══════════════════════════════════════════════════════════════════════════════

DIVIDER = "=" * 60
fs = gcsfs.GCSFileSystem()


# ── helpers ───────────────────────────────────────────────────────────────────

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def nearest_gridpoint(sensor_lat, sensor_lon, grid_lats, grid_lons):
    dists = [haversine_km(sensor_lat, sensor_lon, glat, glon)
             for glat, glon in zip(grid_lats, grid_lons)]
    idx = int(np.argmin(dists))
    return grid_lats[idx], grid_lons[idx], dists[idx]

def coord_key(lat, lon):
    return (round(lat, 5), round(lon, 5))


def main():

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1 — Load and cache FloodNet
    # ═══════════════════════════════════════════════════════════════════════════
    print(f"\n{DIVIDER}")
    print("STEP 1: Load FloodNet data")
    print(DIVIDER)

    print("  Reading FloodNet CSV from GCS...")
    with fs.open(FLOODNET_CSV) as f:
        fn = pd.read_csv(f)
    print(f"  Raw rows: {len(fn):,}  |  Sensors: {fn['deployment_id'].nunique()}")

    # Parse timestamps — mixed format (some have microseconds, some don't)
    fn["time"] = pd.to_datetime(fn["time"], format="ISO8601", utc=True)

    # Floor to minute to align with MRMS 1-min resolution
    fn["time"] = fn["time"].dt.floor("min")

    # Keep only needed columns
    fn = fn[["deployment_id", "name", "time", "depth_proc_mm"]]

    # Average any readings that collide in the same minute after flooring
    # (~every 16 min, sensor clock drift causes two readings in same minute)
    dupes_before = fn.duplicated(subset=["deployment_id", "time"]).sum()
    fn = (fn.groupby(["deployment_id", "time"], as_index=False)
            .agg({"depth_proc_mm": "mean", "name": "first"}))
    print(f"  Duplicate (sensor, minute) pairs averaged: {dupes_before:,}")
    print(f"  Rows after 1-min averaging: {len(fn):,}")
    print(f"  Date range: {fn['time'].min().date()} → {fn['time'].max().date()}")

    # Cache to local VM disk — much faster for repeated access in the loop
    fn.to_parquet(LOCAL_FN_PARQUET, index=False)
    print(f"  Cached locally → {LOCAL_FN_PARQUET}")


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2 — Load sensor metadata and build grid point mapping
    # ═══════════════════════════════════════════════════════════════════════════
    print(f"\n{DIVIDER}")
    print("STEP 2: Match sensors to nearest MRMS grid points")
    print(DIVIDER)

    with fs.open(SENSOR_META_CSV) as f:
        meta = pd.read_csv(f)[["deployment_id", "name", "latitude", "longitude"]].dropna()
    print(f"  Sensors loaded: {len(meta)}")

    # Filter FloodNet to sensors in metadata
    fn = fn[fn["deployment_id"].isin(meta["deployment_id"].values)]
    print(f"  FloodNet rows after sensor filter: {len(fn):,}")

    # Extract MRMS grid geometry from one sample file
    print(f"  Extracting MRMS grid geometry from {GRID_SAMPLE_FILE}...")
    with fs.open(MRMS_DIR + GRID_SAMPLE_FILE) as f:
        sample = pd.read_csv(f, nrows=500_000)
    first_ts  = sample["time"].iloc[0]
    grid_df   = sample[sample["time"] == first_ts][["latitude", "longitude"]].drop_duplicates()
    grid_lats = grid_df["latitude"].tolist()
    grid_lons = grid_df["longitude"].tolist()
    print(f"  Grid points in NYC bounding box: {len(grid_lats)}")

    # Match each sensor to its nearest grid point
    assignments = []
    for _, row in meta.iterrows():
        glat, glon, dist = nearest_gridpoint(
            row["latitude"], row["longitude"], grid_lats, grid_lons
        )
        assignments.append({
            "deployment_id": row["deployment_id"],
            "sensor_name":   row["name"],
            "sensor_lat":    row["latitude"],
            "sensor_lon":    row["longitude"],
            "mrms_lat":      glat,
            "mrms_lon":      glon,
            "dist_km":       round(dist, 4),
        })
    assign_df = pd.DataFrame(assignments)

    # Save gridpoint map to GCS for reference
    with fs.open(OUTPUT_DIR + "sensor_gridpoint_map.csv", "w") as f:
        assign_df.to_csv(f, index=False)

    n_unique = assign_df[["mrms_lat", "mrms_lon"]].drop_duplicates().shape[0]
    print(f"  Unique MRMS grid points needed: {n_unique}  (out of {len(grid_lats)} total)")
    print(f"  Distance stats:  mean={assign_df['dist_km'].mean():.3f} km  "
          f"max={assign_df['dist_km'].max():.3f} km")

    # Build lookup structures
    sensor_to_grid = {
        row["deployment_id"]: (row["mrms_lat"], row["mrms_lon"])
        for _, row in assign_df.iterrows()
    }
    needed_keys = {coord_key(lat, lon) for lat, lon in sensor_to_grid.values()}

    # Add MRMS grid coords to FloodNet
    fn["mrms_lat"] = fn["deployment_id"].map(lambda d: sensor_to_grid.get(d, (None, None))[0])
    fn["mrms_lon"] = fn["deployment_id"].map(lambda d: sensor_to_grid.get(d, (None, None))[1])
    fn = fn.dropna(subset=["mrms_lat", "mrms_lon"])
    fn["mrms_lat_r"] = fn["mrms_lat"].round(5)
    fn["mrms_lon_r"] = fn["mrms_lon"].round(5)


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3 — List MRMS files and find overlap with FloodNet dates
    # ═══════════════════════════════════════════════════════════════════════════
    print(f"\n{DIVIDER}")
    print("STEP 3: Identify overlapping MRMS files")
    print(DIVIDER)

    all_files = fs.ls(MRMS_DIR.replace("gs://", "").rstrip("/"))
    date_to_file = {}
    for f in all_files:
        m = re.search(r"mrms_nyc_1min_(\d{8})\.csv", f)
        if m:
            date_to_file[m.group(1)] = "gs://" + f

    fn_dates     = fn["time"].dt.date.unique()
    target_dates = sorted([d.strftime("%Y%m%d") for d in fn_dates
                           if d.strftime("%Y%m%d") in date_to_file])
    missing      = [d.strftime("%Y%m%d") for d in fn_dates
                    if d.strftime("%Y%m%d") not in date_to_file]

    print(f"  MRMS files available:         {len(date_to_file)}")
    print(f"  FloodNet unique dates:        {len(fn_dates)}")
    print(f"  Overlapping dates to process: {len(target_dates)}")
    if missing:
        print(f"  FloodNet dates missing MRMS:  {len(missing)}")
        print(f"    First 5: {missing[:5]}")


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4 — Loop over days, merge, concatenate
    # ═══════════════════════════════════════════════════════════════════════════
    print(f"\n{DIVIDER}")
    print("STEP 4: Merge MRMS + FloodNet day by day")
    print(DIVIDER)

    merged_chunks = []
    skipped       = 0
    total_rainy   = 0

    for i, date_str in enumerate(target_dates):

        if i == 0 or (i + 1) % 50 == 0:
            pct = (i + 1) / len(target_dates) * 100
            print(f"  [{i+1}/{len(target_dates)} — {pct:.0f}%] Processing {date_str}...")

        try:
            with fs.open(date_to_file[date_str]) as f:
                mrms_day = pd.read_csv(f)
        except Exception as e:
            print(f"    ⚠ Failed to load {date_str}: {e}")
            skipped += 1
            continue

        # Parse timestamps
        mrms_day["time"] = pd.to_datetime(mrms_day["time"], utc=True)

        # Filter to only grid points assigned to at least one sensor (~7% of rows)
        mrms_day["_key"] = list(zip(mrms_day["latitude"].round(5), mrms_day["longitude"].round(5)))
        mrms_day = mrms_day[mrms_day["_key"].isin(needed_keys)].copy()
        mrms_day = mrms_day.drop(columns=["_key"])
        mrms_day = mrms_day.rename(columns={"latitude": "mrms_lat", "longitude": "mrms_lon"})
        mrms_day["mrms_lat_r"] = mrms_day["mrms_lat"].round(5)
        mrms_day["mrms_lon_r"] = mrms_day["mrms_lon"].round(5)

        # FloodNet slice for this date
        date_obj = pd.Timestamp(date_str, tz="UTC").date()
        fn_day   = fn[fn["time"].dt.date == date_obj].copy()

        if fn_day.empty:
            continue

        # Merge on (mrms_lat_r, mrms_lon_r, time)
        merged = fn_day.merge(
            mrms_day[["mrms_lat_r", "mrms_lon_r", "time", "precip_rate_mmhr"]],
            on=["mrms_lat_r", "mrms_lon_r", "time"],
            how="left"
        )

        total_rainy += int((merged["precip_rate_mmhr"] > 0).sum())
        merged_chunks.append(merged)

    print(f"\n  Days processed: {len(target_dates) - skipped}")
    print(f"  Days skipped:   {skipped}")
    print(f"  Total rainy rows accumulated: {total_rainy:,}")


    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 5 — Concatenate, save locally, upload to GCS
    # ═══════════════════════════════════════════════════════════════════════════
    print(f"\n{DIVIDER}")
    print("STEP 5: Concatenate and save")
    print(DIVIDER)

    merged_all = pd.concat(merged_chunks, ignore_index=True)
    merged_all = merged_all.drop(
        columns=["mrms_lat", "mrms_lon", "mrms_lat_r", "mrms_lon_r"], errors="ignore"
    )
    merged_all = merged_all.sort_values(["deployment_id", "time"]).reset_index(drop=True)

    print(f"  Total merged rows:        {len(merged_all):,}")
    print(f"  Sensors represented:      {merged_all['deployment_id'].nunique()}")
    print(f"  MRMS match rate:          {merged_all['precip_rate_mmhr'].notna().mean()*100:.1f}%")
    print(f"  Rows with depth > 0:      {(merged_all['depth_proc_mm'] > 0).sum():,}")
    print(f"  Rows with precip > 0:     {(merged_all['precip_rate_mmhr'] > 0).sum():,}")
    print(f"  Rows with both > 0:       {((merged_all['depth_proc_mm'] > 0) & (merged_all['precip_rate_mmhr'] > 0)).sum():,}")

    # Save locally first then upload
    print(f"\n  Saving to local disk → {LOCAL_MERGED}")
    merged_all.to_parquet(LOCAL_MERGED, index=False)
    size_mb = LOCAL_MERGED.stat().st_size / 1e6
    print(f"  Local file size: {size_mb:.1f} MB")

    print(f"  Uploading to GCS → {OUTPUT_DIR}merged_mrms_floodnet.parquet")
    fs.put(str(LOCAL_MERGED), OUTPUT_DIR.replace("gs://", "") + "merged_mrms_floodnet.parquet")

    print(f"\n{DIVIDER}")
    print("JOIN COMPLETE")
    print(f"Output: {OUTPUT_DIR}merged_mrms_floodnet.parquet")
    print(DIVIDER)


if __name__ == "__main__":
    main()