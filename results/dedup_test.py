import sys, time
sys.argv = ["x"]
sys.path.insert(0, ".")
import psutil, ray
import ray_clean as rc
ray.init(address="auto", logging_level="ERROR")
ds = rc.ingest("data")
total, t0 = 0, time.time()
for b in range(rc.BLOCKS):
    t = time.time()
    n = rc.cleanse_bucket(ds, b).count()
    total += n
    print("bucket %d: %d rows  %.1fs  | available mem %.1f GiB" % (b, n, time.time() - t, psutil.virtual_memory().available / 2**30), flush=True)
print("TOTAL after cleanse+dedup: %d (Spark: 11698059)  %.1fs" % (total, time.time() - t0), flush=True)
ray.shutdown()
