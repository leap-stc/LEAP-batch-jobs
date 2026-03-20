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
