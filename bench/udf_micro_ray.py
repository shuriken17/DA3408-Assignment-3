import json, os, sys, time
os.environ.setdefault("RAY_AUTH_MODE", "disabled")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))
import pyarrow.compute as pc
import ray
import ray_clean as rc

SRC = "out/ray"
COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"]
MODES = ["python", "pandas", "native"]
ray.init(address="auto", logging_level="ERROR")
assert len([x for x in ray.nodes() if x["Alive"]]) == 2, "need 2 Ray nodes"
ds = ray.data.read_parquet(SRC, override_num_blocks=4).select_columns(COLS).materialize()
n = ds.count()
print("ray rows:", n, flush=True)


def run(d, mode):
    out = d.map_batches(rc.UDFS[mode], batch_format="pyarrow", batch_size=rc.BATCH).select_columns(["speed_centi"])
    total = 0
    for tbl in out.iter_batches(batch_format="pyarrow", batch_size=None):
        total += pc.sum(tbl["speed_centi"]).as_py() or 0
    return total


for mode in MODES:
    run(ds.limit(200000), mode)
res = {"framework": "ray", "rows": n, "runs": {}, "checksum": {}}
for rep in range(3):
    k = rep % 3
    for mode in MODES[k:] + MODES[:k]:
        t0 = time.perf_counter()
        total = run(ds, mode)
        dt = time.perf_counter() - t0
        res["runs"].setdefault(mode, []).append(round(dt, 3))
        res["checksum"][mode] = int(total)
        print("ray %-7s rep %d %6.1fs sum=%s" % (mode, rep + 1, dt, total), flush=True)
json.dump(res, open("results/udf_micro_ray.json", "w"), indent=1)
ray.shutdown()
