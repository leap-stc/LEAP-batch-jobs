# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = [
#   "torch",
#   "leap-batch-jobs @ git+https://github.com/leap-stc/LEAP-batch-jobs.git",
# ]
#
# [tool.uv]
# extra-index-url = ["https://download.pytorch.org/whl/cu121"]
# ///

import logging
import torch
from leap_batch_jobs.monitoring import ResourceMonitor, notify_slack

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    if not torch.cuda.is_available():
        raise RuntimeError("No CUDA-capable GPU detected. This script requires a GPU.")

    device_name = torch.cuda.get_device_name(0)
    vram_gb = torch.cuda.get_device_properties(0).total_memory / 1024**3
    logger.info(f"GPU: {device_name} ({vram_gb:.1f} GB VRAM)")

    # TODO: add your GPU workload here


if __name__ == "__main__":
    try:
        with ResourceMonitor():
            main()
    except Exception:
        import traceback
        traceback.print_exc()
        notify_slack("project_name failed — check the logs")
        raise