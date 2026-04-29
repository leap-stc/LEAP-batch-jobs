# LEAP Batch Jobs

Run batch Python jobs on GCP via [SkyPilot](https://docs.skypilot.co) and GitHub Actions. Jobs are dispatched to a managed SkyPilot cluster — you write the script, open a PR, and a maintainer triggers the run.

## How it works

1. You write a `batch.py` and a `config.yml` in a folder under `BATCH_JOBS/`
2. You open a pull request with that folder
3. A maintainer reviews and merges it
4. The maintainer (or you, if you have write access) triggers the job from the **Actions** tab using **Run Batch Job**
5. The job runs on GCP; you monitor it via the SkyPilot dashboard or log streaming

---

> **Never put secrets or credentials in your PR.**
> Everything in `batch.py` and `config.yml` is public — it lives in this repo and is visible to anyone. Hardcoded tokens, passwords, or API keys will be exposed. They will also appear in SkyPilot logs, which are visible on the shared dashboard.
>
> Use environment variables for anything sensitive. If you need a secret injected at run time, ask a maintainer to add it as a GitHub Actions secret and pass it through `job_env` at dispatch — it will never touch the repo.

---

## Submitting a job

### 1. Fork and create a branch

Fork this repo (or create a branch if you have write access), then create a folder:

```
BATCH_JOBS/<your_github_username>/<project_name>/
```

For example: `BATCH_JOBS/jsmith/era5_climatology/`

### 2. Write `batch.py`

Use [PEP 723 inline script metadata](https://docs.astral.sh/uv/guides/scripts/#declaring-script-dependencies) to declare your dependencies at the top of the file — no `requirements.txt` or separate environment needed.

```python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "gcsfs",
#   "xarray",
#   "zarr",
# ]
# ///

import xarray as xr

def main():
    ds = xr.open_dataset("gs://my-bucket/input.zarr", engine="zarr", chunks="auto")
    result = ds.groupby("time.month").mean("time")
    result.to_zarr("gs://leap-scratch/<your_username>/output.zarr", mode="w")

if __name__ == "__main__":
    main()
```

**Tips:**
- Write outputs to `gs://leap-scratch/<your_username>/` or `gs://leap-persistent/` as appropriate. You can find more information on how to write directly to GCS [here](https://leap-stc.github.io/data/data_tools/#writing-to-gcs)
- GCP credentials are provided automatically by the VM — no need to manage tokens
- If you use `obstore` for direct GCS access (faster for large reads), import `gcp_credential_provider` from `leap_batch_jobs.gcs` to handle credentials correctly on the cluster

**Optional monitoring utilities** from `leap-batch-jobs`:

```python
from leap_batch_jobs.monitoring import ProgressLogger, ResourceMonitor, notify_slack

if __name__ == "__main__":
    try:
        with ResourceMonitor():     # logs CPU/RAM/network every 30s
            with ProgressLogger():  # logs dask task progress every 30s
                main()
        notify_slack("my_project finished: gs://leap-scratch/me/output.zarr")
    except Exception:
        import traceback
        traceback.print_exc()
        notify_slack("my_project failed — check the logs")
        raise
```

`notify_slack()` posts to `#leap-batch-jobs`. The webhook URL is injected automatically by the workflow — no configuration needed on your end. It silently does nothing if called outside of a job run (e.g. during local testing).

### 3. Write `config.yml`

This is a [SkyPilot task YAML](https://docs.skypilot.co/en/latest/reference/yaml-spec.html). A minimal config:

```yaml
name: my_job_name

resources:
  cloud: gcp
  region: us-central1
  instance_type: n4-standard-4   # see instance types below
  use_spot: false

workdir: .   # uploads this folder to the remote VM

setup: |
  curl -LsSf https://astral.sh/uv/install.sh | sh

run: |
  set -e
  source $HOME/.local/bin/env
  PYTHONUNBUFFERED=1 uv run batch.py
```

**Available instance types** (can also be overridden at dispatch time):

| Type | vCPU | RAM | Use when |
|---|---|---|---|
| `n4-standard-4` | 4 | 16 GB | Small jobs, light compute |
| `n4-standard-8` | 8 | 32 GB | Medium jobs |
| `n4-standard-16` | 16 | 64 GB | Parallel compute |
| `n4-highmem-16` | 16 | 128 GB | Large in-memory datasets |
| `n4-highmem-32` | 32 | 256 GB | Very large in-memory datasets |

**Environment variables** — if your script reads parameters from env vars, declare defaults in `config.yml`:

```yaml
envs:
  N_YEARS: "10"
  OUTPUT_PATH: "gs://leap-scratch/myuser/output.zarr"
```

These can be overridden at dispatch time without editing the file.

**Local disk** — by default jobs have no extra attached disk. If you need to cache data locally before writing to GCS, add:

```yaml
resources:
  disk_size: 200  # GB
```

Or override it at dispatch time.

### 4. Open a pull request

Push your branch and open a PR. The PR should contain only your `BATCH_JOBS/<user>/<project>/` folder. A maintainer will review your script and config before merging.

---

## Running a job (maintainers / users with write access)

After the PR is merged, go to **Actions → Run Batch Job → Run workflow**.

| Input | Description |
|---|---|
| **job** | Path to the job folder, e.g. `BATCH_JOBS/jsmith/era5_climatology` |
| **instance_type** | Override the instance type from `config.yml`, or leave as `from config` |
| **disk_size_gb** | Attach extra local disk (useful for caching); `none` means no extra disk |
| **job_env** | Space-separated `KEY=VALUE` pairs to override `envs` in `config.yml`, e.g. `N_YEARS=5 OUTPUT_PATH=gs://...` |

The workflow submits the job asynchronously and exits. The job runs on the cluster in the background.

---

## Monitoring your job

After dispatch, the GitHub Actions summary shows:
- The **job ID** assigned by SkyPilot
- A link to the **SkyPilot dashboard** where you can watch status
- A command to **stream logs** from your terminal:

```bash
pip install skypilot
sky api login -e <SKYPILOT_API_URL>
sky jobs logs --name <job-name>
```

The job name follows the pattern `<project_name>-<github_actor>-<run_id>`.

---

## Cancelling a Job

If you need to cancel a running or queued job, connect to the API server first (see Monitoring your job above for how to get the URL and run `sky api login`)

The cancel by job ID:
`sky jobs cancel <job_id> # ex: sky jobs cancel 14`

---

## Repository structure

```
BATCH_JOBS/
  <github_username>/
    <project_name>/
      batch.py       # your script
      config.yml     # SkyPilot task config

examples/
  small_job/         # reads a zarr store, computes a climatology
  large_job/         # ERA5 diurnal temperature range, uses obstore

src/leap_batch_jobs/
  gcs.py             # gcp_credential_provider() for obstore on GCP VMs
  monitoring.py      # ResourceMonitor and ProgressLogger context managers
```

See `examples/small_job/` and `examples/large_job/` for complete working examples.

