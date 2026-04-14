import json
import os
import threading
import time
import urllib.request

import psutil


def notify_slack(message: str, webhook_url: str | None = None) -> None:
    """Post a message to the #leap-batch-jobs Slack channel.

    The webhook URL is injected automatically by GitHub Actions via the
    SLACK_WEBHOOK_URL environment variable. You can also pass it explicitly.
    Does nothing if no URL is available, so it is safe to call unconditionally.

    Usage::

        notify_slack(f"my_project finished: gs://leap-scratch/me/output.zarr")
    """
    url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        return
    payload = json.dumps({"text": message}).encode()
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    except Exception as exc:
        print(f"[notify_slack] failed to send notification: {exc}", flush=True)


def _fmt_elapsed(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}:{s:02d}"


class ResourceMonitor:
    """Context manager that logs CPU, memory, and network I/O to stdout at a fixed interval."""

    def __init__(self, interval: int = 30):
        self.interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        net_before = psutil.net_io_counters()
        t_before = time.monotonic()
        # Prime cpu_percent (first call always returns 0.0)
        psutil.cpu_percent()
        while not self._stop.wait(self.interval):
            cpu = psutil.cpu_percent()
            mem = psutil.virtual_memory()
            net_after = psutil.net_io_counters()
            t_after = time.monotonic()
            elapsed = t_after - t_before
            recv_mb_s = (net_after.bytes_recv - net_before.bytes_recv) / elapsed / 1e6
            sent_mb_s = (net_after.bytes_sent - net_before.bytes_sent) / elapsed / 1e6
            print(
                f"[monitor]  CPU {cpu:.1f}%  "
                f"RAM {mem.used / 1e9:.1f}/{mem.total / 1e9:.1f} GB ({mem.percent:.1f}%)  "
                f"net ↓{recv_mb_s:.0f} ↑{sent_mb_s:.0f} MB/s",
                flush=True,
            )
            net_before = net_after
            t_before = t_after

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join()


def _progress_line(pct: float, completed: int, total: int, elapsed: float) -> str:
    bar_width = 28
    filled = round(pct / 100 * bar_width)
    bar = "█" * filled + "░" * (bar_width - filled)
    return f"[progress] {pct:5.1f}% |{bar}| {completed}/{total} tasks  {_fmt_elapsed(elapsed)} elapsed"


class ProgressLogger:
    """Context manager that logs dask computation progress to stdout periodically.

    Uses dask's callback system to track task completion. Output is one line
    per interval with no carriage-return tricks, so it renders correctly in
    GitHub Actions logs and other non-interactive environments.

    Usage::

        with ProgressLogger(interval=30):
            ds.to_zarr(store, mode="w")
    """

    def __init__(self, interval: int = 30):
        self.interval = interval
        self._total = 0
        self._completed = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._start_time: float | None = None
        self._cb = None

    def _start(self, dsk):
        self._total = len(dsk)
        self._completed = 0
        self._start_time = time.monotonic()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _posttask(self, key, result, dsk, state, worker_id):
        with self._lock:
            self._completed += 1

    def _finish(self, dsk, state, errored):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        if not errored:
            elapsed = time.monotonic() - self._start_time
            print(
                _progress_line(100.0, self._total, self._total, elapsed),
                flush=True,
            )

    def _run(self):
        while not self._stop.wait(self.interval):
            with self._lock:
                completed = self._completed
            total = self._total
            elapsed = time.monotonic() - self._start_time
            pct = (completed / total * 100) if total > 0 else 0
            print(_progress_line(pct, completed, total, elapsed), flush=True)

    def __enter__(self):
        from dask.callbacks import Callback

        self._cb = Callback(
            start=self._start,
            posttask=self._posttask,
            finish=self._finish,
        )
        self._cb.__enter__()
        return self

    def __exit__(self, *args):
        if self._cb is not None:
            self._cb.__exit__(*args)
        # Ensure printer thread stops even if _finish was never called (e.g. on error)
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
