# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "dask[distributed]",
#   "obstore",
#   "xarray",
#   "zarr",
# ]
# ///
import os
from dask.distributed import Client
from obstore.store import GCSStore
from zarr.storage import ObjectStore
import xarray as xr
import zarr

zarr.config.set({"async.concurrency": 128})


def main():
    varlist = [
        "maximum_2m_temperature_since_previous_post_processing",
        "minimum_2m_temperature_since_previous_post_processing",
    ]

    # processes=False uses threads instead of subprocesses, so the obstore
    # ObjectStore (a Rust/pyo3 object) doesn't need to be pickled across workers
    with Client(processes=False, dashboard_address=":8787") as client:
        print(f"Dask dashboard: {client.dashboard_link}")

        # Temporarily clear GOOGLE_APPLICATION_CREDENTIALS so obstore doesn't
        # try to parse the workload identity (external_account) credential file —
        # an unsupported format in obstore — for a public bucket that needs no auth.
        _creds = os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        try:
            gcs_store = GCSStore.from_url(
                "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3/",
                skip_signature=True,
            )
        finally:
            if _creds is not None:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _creds

        zarr_store = ObjectStore(store=gcs_store, read_only=True)
        ds = xr.open_zarr(
            zarr_store,
            consolidated=False,
            chunks={"time": 30, "latitude": 721, "longitude": 1440},
        ).drop_encoding()
        ds = ds[varlist]
        ds["diurnal_temperature"] = (
            ds["maximum_2m_temperature_since_previous_post_processing"]
            - ds["minimum_2m_temperature_since_previous_post_processing"]
        )
        ds[["diurnal_temperature"]].to_zarr(
            "gs://leap-scratch/large_job_example.zarr", mode="w"
        )


if __name__ == "__main__":
    main()
