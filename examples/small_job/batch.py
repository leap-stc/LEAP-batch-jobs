# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "gcsfs",
#   "leap-batch-jobs @ git+https://github.com/leap-stc/LEAP-batch-jobs.git",
#   "xarray",
#   "zarr",
# ]
# ///

import xarray as xr

from leap_batch_jobs.monitoring import ProgressLogger, ResourceMonitor


def main():
    store = "gs://leap-persistent/data-library/GPCP-daily/GPCP-daily.zarr"
    ds = xr.open_dataset(store, engine="zarr", chunks="auto")

    climatology = ds.groupby("time.month").mean("time")
    with ProgressLogger():
        climatology.to_zarr("gs://leap-scratch/batch-test.zarr", mode="w")


if __name__ == "__main__":
    try:
        with ResourceMonitor():
            main()
    except Exception:
        import traceback

        traceback.print_exc()
        raise
