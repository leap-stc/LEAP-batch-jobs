#!/usr/bin/env python3
"""
launch_mrms_jobs.py
-------------------
Splits the full MRMS date range into chunks and launches one SkyPilot job
per chunk in parallel. Each job writes its own days to GCS independently.
Resume-safe: re-running skips already-completed days automatically.

Usage:
    python launch_mrms_jobs.py                        # default: 2020-10-01 to today, 20 nodes
    python launch_mrms_jobs.py --nodes 10             # fewer nodes
    python launch_mrms_jobs.py --start 2022-01-01     # partial range
    python launch_mrms_jobs.py --dry-run              # print jobs without launching
"""

import argparse
import subprocess
from datetime import date, datetime, timedelta

# ── Config ─────────────────────────────────────────────────────────────────────
MRMS_START  = date(2020, 10, 1)   # earliest date in noaa-mrms-pds S3 bucket
MRMS_END    = date.today()
DEFAULT_NODES = 20                 # 20 nodes × ~110 days each ≈ 2–4 hours total
WORKERS_PER_NODE = 64             # threads per node (n2-highcpu-32 handles this fine)
YAML_FILE   = "mrms_sky.yaml"

# ──────────────────────────────────────────────────────────────────────────────

def split_date_range(start: date, end: date, n_chunks: int) -> list[tuple[date, date]]:
    """Split [start, end] into n_chunks roughly equal date ranges."""
    total_days = (end - start).days + 1
    chunk_size = max(1, total_days // n_chunks)
    chunks = []
    current = start
    for i in range(n_chunks):
        chunk_end = min(current + timedelta(days=chunk_size - 1), end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
        if current > end:
            break
    return chunks


def launch_job(job_name: str, start: date, end: date, dry_run: bool):
    cmd = [
        "sky", "launch",
        "--yes",                        # don't prompt for confirmation
        "--detach-run",                 # fire and forget — don't block
        "--name", job_name,
        "--env", f"START_DATE={start.strftime('%Y-%m-%d')}",
        "--env", f"END_DATE={end.strftime('%Y-%m-%d')}",
        "--env", f"WORKERS={WORKERS_PER_NODE}",
        YAML_FILE,
    ]
    print(f"  {job_name}: {start} -> {end}  ({(end-start).days+1} days)")
    if dry_run:
        print(f"    [DRY RUN] would run: {' '.join(cmd)}")
        return
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print(f"  WARNING: {job_name} launch failed (returncode={result.returncode})")


def main():
    parser = argparse.ArgumentParser(description="Launch parallel SkyPilot MRMS jobs")
    parser.add_argument("--start",   default=MRMS_START.strftime("%Y-%m-%d"),
                        help=f"Start date (default: {MRMS_START})")
    parser.add_argument("--end",     default=MRMS_END.strftime("%Y-%m-%d"),
                        help=f"End date (default: today)")
    parser.add_argument("--nodes",   type=int, default=DEFAULT_NODES,
                        help=f"Number of parallel VMs (default: {DEFAULT_NODES})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print jobs without launching")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end   = datetime.strptime(args.end,   "%Y-%m-%d").date()
    total_days = (end - start).days + 1

    chunks = split_date_range(start, end, args.nodes)
    n_jobs = len(chunks)

    # Estimate runtime
    # ~14 min/day on JupyterHub, but co-located GCP->S3 should be ~2-3 min/day
    est_min = max(c[1] - c[0] for c in chunks).days * 2
    est_max = max(c[1] - c[0] for c in chunks).days * 4

    print("=" * 60)
    print(f"  MRMS SkyPilot launcher")
    print(f"  Date range  : {start} -> {end}  ({total_days} days)")
    print(f"  Nodes       : {n_jobs} parallel VMs")
    print(f"  Days/node   : ~{total_days // n_jobs}")
    print(f"  Workers/node: {WORKERS_PER_NODE} threads")
    print(f"  Est. runtime: ~{est_min}-{est_max} min  (all nodes in parallel)")
    print(f"  Dry run     : {args.dry_run}")
    print("=" * 60)

    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        job_name = f"mrms-{chunk_start.strftime('%Y%m%d')}-{chunk_end.strftime('%Y%m%d')}"
        launch_job(job_name, chunk_start, chunk_end, dry_run=args.dry_run)

    print()
    if args.dry_run:
        print("Dry run complete. Remove --dry-run to launch.")
    else:
        print(f"Launched {n_jobs} jobs. Monitor with:")
        print(f"  sky queue")
        print(f"  sky logs <job-name>")
        print(f"Check GCS output with:")
        print(f"  gsutil ls gs://leap-persistent/nyc_flooding/mrms/ | wc -l")


if __name__ == "__main__":
    main()