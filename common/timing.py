import json, os, time
from contextlib import contextmanager

class StageTimer:
    """Records wall-clock seconds per pipeline stage. Used identically by spark_clean.py and ray_clean.py."""
    def __init__(self, framework, udf_mode, run_id):
        self.meta = {"framework": framework, "udf_mode": udf_mode, "run_id": run_id}
        self.stages = []
        self.counts = {}
        self.t_start = None

    def start(self):
        self.t_start = time.perf_counter()

    @contextmanager
    def stage(self, name):
        t0 = time.perf_counter()
        yield
        self.stages.append({"stage": name, "seconds": round(time.perf_counter() - t0, 3)})

    def count(self, name, value):
        self.counts[name] = int(value)

    def save(self, extra=None, out_dir="results"):
        total = round(time.perf_counter() - self.t_start, 3)
        rec = {**self.meta, "total_seconds": total, "stages": self.stages, "row_counts": self.counts}
        if extra:
            rec.update(extra)
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, "%s_%s_%s.json" % (self.meta["framework"], self.meta["udf_mode"], self.meta["run_id"]))
        with open(path, "w") as f:
            json.dump(rec, f, indent=2)
        print("saved", path, "| total %.2fs" % total)
        return path
