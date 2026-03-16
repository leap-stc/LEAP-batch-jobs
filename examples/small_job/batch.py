# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "xarray",
#   "zarr",
#   "gcsfs",
# ]
# ///

import xarray as xr


def main():
    store = "gs://leap-persistent/data-library/GPCP-daily/GPCP-daily.zarr"
    ds = xr.open_dataset(store, engine="zarr", chunks="auto")

    climatology = ds.groupby("time.month").mean("time")
    climatology.to_zarr("gs://leap-scratch/batch-test.zarr", mode="w")


if __name__ == "__main__":
    main()
