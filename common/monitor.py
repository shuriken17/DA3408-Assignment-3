import argparse, csv, os, signal, time
import psutil

PATTERNS = ("spark", "raylet", "gcs_server", "ray::", "/ray/", "ray_clean")
stop = False


def _stop(*_):
    global stop
    stop = True


def cluster_rss():
    """Summed resident memory (bytes) of Spark and Ray processes only."""
    total = 0
    for p in psutil.process_iter(["cmdline", "memory_info"]):
        try:
            cmd = " ".join(p.info["cmdline"] or []).lower()
            if "monitor.py" in cmd or "run_bench.py" in cmd:
                continue
            if any(k in cmd for k in PATTERNS):
                total += p.info["memory_info"].rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=float, default=1.0)
    a = ap.parse_args()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    psutil.cpu_percent(interval=None)
    rows, t0 = [], time.time()
    with open(a.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t_sec", "cpu_pct", "sys_mem_used_mb", "cluster_rss_mb"])
        while not stop:
            time.sleep(a.interval)
            vm = psutil.virtual_memory()
            row = [round(time.time() - t0, 1), psutil.cpu_percent(interval=None),
                   round((vm.total - vm.available) / 2**20), round(cluster_rss() / 2**20)]
            w.writerow(row)
            f.flush()
            rows.append(row)
    if rows:
        cpu = [r[1] for r in rows]
        print("monitor: %d samples | CPU peak %.0f%% mean %.0f%% | system memory peak %d MB | Spark/Ray RSS peak %d MB"
              % (len(rows), max(cpu), sum(cpu) / len(cpu), max(r[2] for r in rows), max(r[3] for r in rows)))


if __name__ == "__main__":
    main()
