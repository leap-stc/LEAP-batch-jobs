# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "dadapy",
#   "numpy<2",
#   "scipy<1.15",
#   "gcsfs",
#   "leap-batch-jobs @ git+https://github.com/leap-stc/LEAP-batch-jobs.git",
#   "numpy",
#   "xarray",
#   "zarr",
#   "pandas",
#   "scikit-learn"
# ]
# ///

import os
import numpy as np
import xarray as xr
import pandas as pd
import gcsfs
from sklearn.preprocessing import StandardScaler
from dadapy.feature_weighting import FeatureWeighting
from leap_batch_jobs.monitoring import ResourceMonitor


def main():
    n = int(os.environ.get("N", 16000))
    random_state = int(os.environ.get("RANDOM_STATE", 10))
    n_epochs = int(os.environ.get("N_EPOCHS", 80))
    output_path = os.environ.get(
        "OUTPUT_PATH", "gs://leap-persistent/dadapy/SOCAT"
    )
    fs = gcsfs.GCSFileSystem()

    SOCAT_mask = xr.open_dataset(
        "gs://leap-persistent/abbysh/zarr_files_/socat_mask_feb1982-dec2022.zarr",
        engine="zarr",
    )
    df = xr.open_dataset(
        "gs://leap-persistent/fayamanda/reconstructions/pCO2_LEAP_fco2-residual-full-dataset-preML_198201-202412.zarr",
        engine="zarr",
    )
    df = df.sel(time=slice("1982-02-01", "2022-12-31"))

    aligned_SOCAT_mask = SOCAT_mask.reindex(time=df.time, method="nearest")
    df_SM = df.where(aligned_SOCAT_mask.socat_mask == 1)

    feature_sel = [
        "sst",
        "sst_anomaly",
        "sss",
        "sss_anomaly",
        "chl_log",
        "chl_log_anomaly",
        "mld_log",
        "xco2_trend",
        "A",
        "B",
        "C",
        "T0",
        "T1",
    ]
    target_sel = ["delta_fco2_1D"]

    df_SM["delta_fco2_1D"] = df_SM["fco2"] - df_SM["xco2_trend"]
    df_SM = df_SM[feature_sel + target_sel]

    df_SM_sel = df_SM.sel(time=slice("2020-01-01", "2022-12-31"))
    SOCAT = df_SM_sel.to_dataframe().dropna()

    SOCAT_sample = SOCAT.sample(n=n, random_state=random_state)

    src_scaler = StandardScaler()
    SOCAT_scaled_sample = src_scaler.fit_transform(
        SOCAT_sample.loc[:, feature_sel + target_sel]
    )
    SOCAT_scaled_sample = pd.DataFrame(
        SOCAT_scaled_sample, columns=feature_sel + target_sel
    )

    gtf = FeatureWeighting(SOCAT_scaled_sample[target_sel].to_numpy(), verbose=True)
    insf = FeatureWeighting(SOCAT_scaled_sample[feature_sel].to_numpy(), verbose=True)

    final_imbs, final_weights = insf.return_backward_greedy_dii_elimination(
        target_data=gtf,
        initial_weights=None,
        n_epochs=n_epochs,
        learning_rate=None,
        decaying_lr="cos",
    )

    weights_path = f"{output_path}_n{n}_rs{random_state}_weights.txt"
    imbs_path = f"{output_path}_n{n}_rs{random_state}_imbs.txt"

    with fs.open(weights_path, "w") as f:
        np.savetxt(f, final_weights)
    with fs.open(imbs_path, "w") as f:
        np.savetxt(f, final_imbs)

    print(f"Saved weights to {weights_path}")
    print(f"Saved imbs to {imbs_path}")


if __name__ == "__main__":
    try:
        with ResourceMonitor():
            main()
    except Exception:
        import traceback

        traceback.print_exc()
        raise
