import argparse, glob, math, os, shutil, sys, time
from collections import defaultdict

os.environ.setdefault("RAY_AUTH_MODE", "disabled")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "common"))
from timing import StageTimer

import numpy as np
import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.compute as pc
import ray

BLOCKS = 8                 # hash buckets (= Spark's 8 shuffle partitions)
BLOCKS_PER_FILE = 2        # 4 files x 2 read blocks
BATCH = 10000              # rows per batch, mirrors Spark's Arrow batch size

KEEP = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
        "PULocationID", "DOLocationID", "fare_amount", "total_amount"]
OUT_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
            "PULocationID", "DOLocationID", "fare_amount", "total_amount", "pickup_date", "pickup_hour",
            "pu_borough", "pu_zone", "do_borough", "do_zone", "avg_speed_mph", "speed_centi",
            "zh_trip_count", "zh_fare_cents_sum", "zh_speed_centi_sum", "zh_speed_n"]
FINAL_SCHEMA = pa.schema([
    ("tpep_pickup_datetime", pa.timestamp("us")), ("tpep_dropoff_datetime", pa.timestamp("us")),
    ("passenger_count", pa.int64()), ("trip_distance", pa.float64()),
    ("PULocationID", pa.int32()), ("DOLocationID", pa.int32()),
    ("fare_amount", pa.float64()), ("total_amount", pa.float64()),
    ("pickup_date", pa.date32()), ("pickup_hour", pa.int32()),
    ("pu_borough", pa.string()), ("pu_zone", pa.string()),
    ("do_borough", pa.string()), ("do_zone", pa.string()),
    ("avg_speed_mph", pa.float64()), ("speed_centi", pa.int64()),
    ("zh_trip_count", pa.int64()), ("zh_fare_cents_sum", pa.int64()),
    ("zh_speed_centi_sum", pa.int64()), ("zh_speed_n", pa.int64()),
])


# ---------------- Spec section 1: ingest ----------------
def canon(tbl: pa.Table) -> pa.Table:
    return pa.table({
        "tpep_pickup_datetime": pc.floor_temporal(tbl["tpep_pickup_datetime"], unit="second"),
        "tpep_dropoff_datetime": pc.floor_temporal(tbl["tpep_dropoff_datetime"], unit="second"),
        "passenger_count": tbl["passenger_count"].cast(pa.int64()),
        "trip_distance": tbl["trip_distance"].cast(pa.float64()),
        "PULocationID": tbl["PULocationID"].cast(pa.int32()),
        "DOLocationID": tbl["DOLocationID"].cast(pa.int32()),
        "fare_amount": tbl["fare_amount"].cast(pa.float64()),
        "total_amount": tbl["total_amount"].cast(pa.float64()),
    })


def read_one(path):
    ds = ray.data.read_parquet(path, override_num_blocks=BLOCKS_PER_FILE).select_columns(KEEP)
    return ds.map_batches(canon, batch_format="pyarrow", batch_size=BATCH)


def ingest(input_dir):
    files = sorted(glob.glob(os.path.join(input_dir, "*.parquet")))
    parts = [read_one(f) for f in files]
    return parts[0].union(*parts[1:])


# ---------------- Spec section 2: cleanse (rules 1-6 and 8; rule 7 is the bucketed dedup) ----------------
def cleanse_batch(tbl: pa.Table) -> pa.Table:
    tbl = tbl.drop_null()                                                              # 1
    pu, do = tbl["tpep_pickup_datetime"], tbl["tpep_dropoff_datetime"]
    lo = pa.scalar(np.datetime64("2023-01-01T00:00:00", "us"), pa.timestamp("us"))
    hi = pa.scalar(np.datetime64("2023-05-01T00:00:00", "us"), pa.timestamp("us"))
    m = pc.and_(pc.greater_equal(pu, lo), pc.less(pu, hi))                             # 2
    m = pc.and_(m, pc.greater(do, pu))                                                 # 3
    m = pc.and_(m, pc.and_(pc.greater(tbl["trip_distance"], 0.0),
                           pc.greater_equal(tbl["fare_amount"], 0.0)))                 # 4
    p = tbl["passenger_count"]
    m = pc.and_(m, pc.and_(pc.greater_equal(p, 1), pc.less_equal(p, 8)))               # 5
    for c in ("PULocationID", "DOLocationID"):                                         # 6
        m = pc.and_(m, pc.and_(pc.greater_equal(tbl[c], 1), pc.less_equal(tbl[c], 263)))
    tbl = tbl.filter(m)
    pu = tbl["tpep_pickup_datetime"]
    tbl = tbl.append_column("pickup_date", pc.cast(pu, pa.date32()))                   # 8
    return tbl.append_column("pickup_hour", pc.cast(pc.hour(pu), pa.int32()))


def _bucket_ids(tbl: pa.Table):
    """Bucket = hash of the Join B key (zone, date, hour). Identical rows share a key, so they share a bucket."""
    pu = tbl["PULocationID"].to_numpy(zero_copy_only=False).astype(np.int64)
    days = pc.cast(tbl["pickup_date"], pa.int32()).to_numpy(zero_copy_only=False).astype(np.int64)
    hr = tbl["pickup_hour"].to_numpy(zero_copy_only=False).astype(np.int64)
    key = (pu * 1_000_000 + days) * 24 + hr
    return (pd.util.hash_array(key) % BLOCKS).astype(np.int64)


def keep_bucket(tbl: pa.Table, b: int) -> pa.Table:
    return tbl.filter(pa.array(_bucket_ids(tbl) == b))


def dedup_block(tbl: pa.Table) -> pa.Table:
    """Spec rule 7: exact duplicates over the 8 kept columns, on one whole bucket."""
    df = tbl.to_pandas().drop_duplicates(subset=KEEP, keep="first")
    return pa.Table.from_pandas(df, schema=tbl.schema, preserve_index=False)


# ---------------- Spec section 3: Join A (lookup against an object put once into the object store) ----------------
def make_zone_arrays(zones_path):
    z = pd.read_csv(zones_path, keep_default_na=False)
    n = int(z.LocationID.max()) + 1
    present = np.zeros(n, dtype=bool)
    borough = np.empty(n, dtype=object)
    zone = np.empty(n, dtype=object)
    for r in z.itertuples():
        present[r.LocationID] = True
        borough[r.LocationID] = r.Borough
        zone[r.LocationID] = r.Zone
    return present, borough, zone


def add_zones(tbl: pa.Table, zone_ref=None) -> pa.Table:
    present, borough, zone = ray.get(zone_ref) if isinstance(zone_ref, ray.ObjectRef) else zone_ref
    pu = tbl["PULocationID"].to_numpy(zero_copy_only=False).astype(np.int64)
    do = tbl["DOLocationID"].to_numpy(zero_copy_only=False).astype(np.int64)
    n = len(present)
    ok = (pu < n) & (do < n)
    ok = ok & present[np.where(ok, pu, 0)] & present[np.where(ok, do, 0)]             # inner join
    tbl, pu, do = tbl.filter(pa.array(ok)), pu[ok], do[ok]
    return (tbl.append_column("pu_borough", pa.array(borough[pu], pa.string()))
               .append_column("pu_zone", pa.array(zone[pu], pa.string()))
               .append_column("do_borough", pa.array(borough[do], pa.string()))
               .append_column("do_zone", pa.array(zone[do], pa.string())))


# ---------------- Spec section 4: speed UDF in three modes ----------------
def _duration_sec(tbl):
    pu = pc.cast(tbl["tpep_pickup_datetime"], pa.int64()).to_numpy(zero_copy_only=False)
    do = pc.cast(tbl["tpep_dropoff_datetime"], pa.int64()).to_numpy(zero_copy_only=False)
    return (do - pu) // 1_000_000


def speed_centi_py(dist, dur):
    if dist is None or dur is None or dur < 60:
        return None
    sp = dist / (dur / 3600.0)
    if sp > 100.0:
        return None
    return int(math.floor(sp * 100.0 + 0.5))


def _with_speed(tbl, sc):
    sc = pa.chunked_array([sc]) if isinstance(sc, pa.Array) else sc
    avg = pc.divide(pc.cast(sc, pa.float64()), 100.0)
    return tbl.append_column("speed_centi", sc).append_column("avg_speed_mph", avg)


def udf_python(tbl):
    dist, dur = tbl["trip_distance"].to_pylist(), _duration_sec(tbl).tolist()
    return _with_speed(tbl, pa.array([speed_centi_py(d, s) for d, s in zip(dist, dur)], pa.int64()))


def udf_pandas(tbl):
    dist = tbl["trip_distance"].to_pandas()
    dur = pd.Series(_duration_sec(tbl))
    sp = dist / (dur / 3600.0)
    ok = (dur >= 60) & (sp <= 100.0)
    sc = np.floor(sp * 100.0 + 0.5).where(ok).astype("Int64")
    return _with_speed(tbl, pa.array(sc, type=pa.int64(), from_pandas=True))


def udf_native(tbl):
    dur = pa.array(_duration_sec(tbl), pa.int64())
    sp = pc.divide(tbl["trip_distance"], pc.divide(pc.cast(dur, pa.float64()), 3600.0))
    ok = pc.and_(pc.greater_equal(dur, 60), pc.less_equal(sp, 100.0))
    val = pc.floor(pc.add(pc.multiply(sp, 100.0), 0.5)).cast(pa.int64(), safe=False)
    return _with_speed(tbl, pc.if_else(ok, val, pa.scalar(None, pa.int64())))


UDFS = {"python": udf_python, "pandas": udf_pandas, "native": udf_native}


# ---------------- Spec section 5: Join B (all rows of a group are inside one bucket) ----------------
def join_heavy_block(tbl: pa.Table) -> pa.Table:
    keys = ["PULocationID", "pickup_date", "pickup_hour"]
    fare_cents = pc.floor(pc.add(pc.multiply(tbl["fare_amount"], 100.0), 0.5)).cast(pa.int64(), safe=False)
    t = (tbl.append_column("_fare_cents", fare_cents)
            .append_column("_one", pa.array(np.ones(len(tbl), dtype=np.int64))))
    agg = t.group_by(keys).aggregate([("_one", "sum"), ("_fare_cents", "sum"),
                                      ("speed_centi", "sum"), ("speed_centi", "count")])
    names = {"_one_sum": "zh_trip_count", "_fare_cents_sum": "zh_fare_cents_sum",
             "speed_centi_sum": "zh_speed_centi_sum", "speed_centi_count": "zh_speed_n"}
    agg = agg.rename_columns([names.get(n, n) for n in agg.column_names])
    i = agg.column_names.index("zh_speed_centi_sum")
    agg = agg.set_column(i, "zh_speed_centi_sum", pc.fill_null(agg["zh_speed_centi_sum"], 0))   # never null
    out = t.join(agg, keys=keys, join_type="inner")
    return out.select(OUT_COLS).cast(FINAL_SCHEMA)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data")
    ap.add_argument("--zones", default="data/taxi_zone_lookup.csv")
    ap.add_argument("--output", default="out/ray")
    ap.add_argument("--udf-mode", default="python", choices=list(UDFS))
    ap.add_argument("--run-id", default="test")
    ap.add_argument("--instrument", action="store_true",
                    help="materialise and count after every stage of every bucket (per-stage timings)")
    ap.add_argument("--only-bucket", type=int, default=None, help="test on a single bucket")
    args = ap.parse_args()
    UDF_BLOCKS = 2             # blocks processed at once in the UDF stage (4 hit the memory limit in native mode)

    ray.init(address="auto", logging_level="ERROR")
    alive = [n for n in ray.nodes() if n["Alive"]]
    cpus = ray.cluster_resources().get("CPU", 0)
    print("ray:", ray.__version__, "| nodes:", len(alive), "| CPUs:", cpus, "| udf:", args.udf_mode,
          "| instrument:", args.instrument, "| only-bucket:", args.only_bucket, flush=True)
    assert len(alive) == 2, "expected 2 Ray nodes, found %d" % len(alive)
    assert cpus == 4, "expected 4 CPUs in the cluster, found %s" % cpus

    shutil.rmtree(args.output, ignore_errors=True)
    os.makedirs(args.output)
    timer = StageTimer("ray", args.udf_mode, args.run_id + ("_inst" if args.instrument else "_e2e"))
    timer.start()

    zone_ref = ray.put(make_zone_arrays(args.zones))
    secs, cnts = defaultdict(float), defaultdict(int)
    buckets = [args.only_bucket] if args.only_bucket is not None else list(range(BLOCKS))

    for b in buckets:
        def run(name, build):
            t0 = time.perf_counter()
            ds = build()
            n = None
            if args.instrument:
                ds = ds.materialize()
                n = ds.count()
                cnts[name] += n
            dt_ = time.perf_counter() - t0
            secs[name] += dt_
            if args.instrument:
                print("bucket %d %-15s %6.1fs rows=%-9s avail %.1f GiB" % (
                    b, name, dt_, n, psutil.virtual_memory().available / 2**30), flush=True)
            return ds

        tb = time.perf_counter()
        ds = run("ingest_cleanse", lambda: ingest(args.input)
                 .map_batches(cleanse_batch, batch_format="pyarrow", batch_size=BATCH)
                 .map_batches(keep_bucket, batch_format="pyarrow", batch_size=BATCH, fn_kwargs={"b": b}))
        ds = run("dedup", lambda: ds.repartition(1).map_batches(dedup_block, batch_format="pyarrow", batch_size=None))
        ds = run("join_zones", lambda: ds.map_batches(add_zones, batch_format="pyarrow", batch_size=None,
                                                      fn_kwargs={"zone_ref": zone_ref}))
        ds = run("apply_udf", lambda: ds.repartition(UDF_BLOCKS).map_batches(UDFS[args.udf_mode], batch_format="pyarrow",
                                                                    batch_size=BATCH))
        ds = run("join_heavy", lambda: ds.repartition(1).map_batches(join_heavy_block, batch_format="pyarrow",
                                                                     batch_size=None))
        t0 = time.perf_counter()
        ds.write_parquet(args.output)
        secs["export"] += time.perf_counter() - t0
        print("bucket %d done in %.1fs | avail %.1f GiB" % (b, time.perf_counter() - tb,
                                                           psutil.virtual_memory().available / 2**30), flush=True)
        del ds

    timer.stages = [{"stage": k, "seconds": round(v, 3)} for k, v in secs.items()]
    for k, v in cnts.items():
        timer.count("after_" + k, v)
    timer.save(extra={"config": {"nodes": len(alive), "cpus": cpus, "buckets": BLOCKS, "batch": BATCH,
                                  "only_bucket": args.only_bucket, "udf_blocks": UDF_BLOCKS, "ray_version": ray.__version__,
                                  "instrument": args.instrument}})
    print("stage totals:", {k: round(v, 1) for k, v in secs.items()}, flush=True)
    print("row totals:", dict(cnts), flush=True)
    ray.shutdown()


if __name__ == "__main__":
    main()
