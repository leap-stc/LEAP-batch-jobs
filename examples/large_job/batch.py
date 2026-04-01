# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "dask",
#   "flox",
#   "gcsfs",
#   "leap-batch-jobs @ git+https://github.com/leap-stc/LEAP-batch-jobs.git",
#   "obstore>=0.9.2",
#   "psutil",
#   "xarray",
#   "zarr",
# ]
# ///
from obstore.store import GCSStore
from zarr.storage import ObjectStore
import xarray as xr
import zarr

from leap_batch_jobs.gcs import gcp_credential_provider
from leap_batch_jobs.monitoring import ProgressLogger, ResourceMonitor

zarr.config.set({"async.concurrency": 128})


def main():
    varlist = [
        "maximum_2m_temperature_since_previous_post_processing",
        "minimum_2m_temperature_since_previous_post_processing",
    ]

    # We could toss this in a function

    # skip_signature=True disables request signing for this public bucket.
    # credential_provider bypasses native credential discovery (env vars, ADC
    # file, instance metadata) which otherwise fails when the VM uses Workload
    # Identity Federation (external_account format, unsupported by obstore).
    # The provider is never actually called because skip_signature prevents
    # any credential fetching.
    gcs_store = GCSStore.from_url(
        "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3/",
        skip_signature=True,
        credential_provider=lambda: {"token": "", "expires_at": None},
    )

    zarr_store = ObjectStore(store=gcs_store, read_only=True)
    ds = xr.open_zarr(
        zarr_store,
        consolidated=False,
        chunks={"time": 30, "latitude": 721, "longitude": 1440},
    ).drop_encoding()
    ds = ds[varlist]
    ds = ds.sel(time=slice("2000-01-01", "2010-01-01"))
    ds["diurnal_temperature"] = (
        ds["maximum_2m_temperature_since_previous_post_processing"]
        - ds["minimum_2m_temperature_since_previous_post_processing"]
    )

    ds = ds[["diurnal_temperature"]].chunk(
        {"time": 30, "latitude": 721, "longitude": 1440}
    )

    write_store = GCSStore.from_url(
        "gs://leap-scratch/leap-batch-job-examples/large-job-obstore.zarr",
        credential_provider=gcp_credential_provider,
    )
    write_zarr_store = ObjectStore(store=write_store)

    with ProgressLogger():
        ds.to_zarr(write_zarr_store, mode="w")


if __name__ == "__main__":
    try:
        with ResourceMonitor():
            main()
    except Exception:
        import traceback

        traceback.print_exc()
        raise
