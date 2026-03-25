import threading
import time

import psutil


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
                f"[monitor] CPU: {cpu:.1f}%  "
                f"RAM: {mem.used / 1e9:.1f}/{mem.total / 1e9:.1f} GB ({mem.percent:.1f}%)  "
                f"net recv: {recv_mb_s:.1f} MB/s  sent: {sent_mb_s:.1f} MB/s",
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
                f"[progress] 100% ({self._total}/{self._total} tasks, elapsed: {elapsed:.0f}s)",
                flush=True,
            )

    def _run(self):
        while not self._stop.wait(self.interval):
            with self._lock:
                completed = self._completed
            total = self._total
            elapsed = time.monotonic() - self._start_time
            pct = (completed / total * 100) if total > 0 else 0
            print(
                f"[progress] {pct:.1f}% ({completed}/{total} tasks, elapsed: {elapsed:.0f}s)",
                flush=True,
            )

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
