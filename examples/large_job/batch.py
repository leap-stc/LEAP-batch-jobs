# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "xarray",
#   "zarr",
#   "obstore",
#   "gcsfs",
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

    gcs_store = GCSStore.from_url(
        "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3/",
        skip_signature=True,
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
    ds[["diurnal_temperature"]].to_zarr(
        "gs://leap-scratch/large_job_example.zarr", mode="w"
    )


if __name__ == "__main__":
    main()
