# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "dask",
#   "flox",
#   "gcsfs",
#   "obstore>=0.9.2",
#   "xarray",
#   "zarr",
# ]
# ///
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
    ds["diurnal_temperature"] = (
        ds["maximum_2m_temperature_since_previous_post_processing"]
        - ds["minimum_2m_temperature_since_previous_post_processing"]
    )

    ds[["diurnal_temperature"]].to_zarr("gs://leap-scratch/leap-batch-job-examples/large-job.zarr", mode="w"
    )


if __name__ == "__main__":
    main()
