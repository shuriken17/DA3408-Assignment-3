import argparse, os, shutil, signal, subprocess, sys, time
import psutil

ap = argparse.ArgumentParser()
ap.add_argument("--framework", required=True, choices=["spark", "ray"])
ap.add_argument("--reps", type=int, default=3)
ap.add_argument("--modes", default="python,pandas,native")
a = ap.parse_args()
fw = a.framework
script = {"spark": "spark_clean.py", "ray": "ray_clean.py"}[fw]
modes = a.modes.split(",")
out = "out/bench_" + fw
os.makedirs("results/monitor", exist_ok=True)
os.makedirs("results/bench_logs", exist_ok=True)


def wait_mem(min_gib=2.0, max_wait=120):
    t = 0
    while psutil.virtual_memory().available / 2**30 < min_gib and t < max_wait:
        time.sleep(5)
        t += 5


for rep in range(1, a.reps + 1):
    k = (rep - 1) % len(modes)
    for mode in modes[k:] + modes[:k]:          # rotate the mode order between repetitions
        run_id = "bench%d" % rep
        name = "%s_%s_%s" % (fw, mode, run_id)
        res = "results/%s_e2e.json" % name
        if os.path.exists(res):
            print("skip (already done):", name, flush=True)
            continue
        wait_mem()
        shutil.rmtree(out, ignore_errors=True)
        sumf = open("results/monitor/%s.summary.txt" % name, "w")
        mon = subprocess.Popen([sys.executable, "common/monitor.py", "--out", "results/monitor/%s.csv" % name], stdout=sumf)
        top = subprocess.Popen(["top", "-b", "-d", "2"], stdout=open("results/monitor/%s.top" % name, "w"),
                               stderr=subprocess.DEVNULL)
        log = open("results/bench_logs/%s.log" % name, "w")
        t0 = time.time()
        try:
            rc = subprocess.run([sys.executable, "-u", script, "--udf-mode", mode, "--run-id", run_id,
                                 "--output", out], stdout=log, stderr=subprocess.STDOUT, timeout=2400).returncode
        except subprocess.TimeoutExpired:
            rc = -9
        wall = time.time() - t0
        mon.send_signal(signal.SIGINT)
        mon.wait(timeout=30)
        top.terminate()
        top.wait(timeout=10)
        sumf.close()
        log.close()
        shutil.rmtree(out, ignore_errors=True)
        ok = os.path.exists(res)
        print("%s rc=%s wall=%.0fs result=%s | %s" % (name, rc, wall, "ok" if ok else "MISSING",
              open("results/monitor/%s.summary.txt" % name).read().strip()), flush=True)
        if not ok:
            print("run failed; stopping. Read results/bench_logs/%s.log" % name, flush=True)
            sys.exit(1)
        time.sleep(15)
print("ALL DONE", flush=True)
