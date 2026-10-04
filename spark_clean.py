import argparse, glob, math, os, sys
from functools import reduce

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "common"))
from timing import StageTimer

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import numpy as np
import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import LongType

MASTER = "spark://127.0.0.1:7077"
PARTITIONS = 8

KEEP = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
        "PULocationID", "DOLocationID", "fare_amount", "total_amount"]


def build_session(udf_mode):
    b = (SparkSession.builder.master(MASTER).appName("a3-spark-clean")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.driver.memory", "1g")
         .config("spark.executor.memory", "1g")
         .config("spark.executor.cores", "2")
         .config("spark.cores.max", "4")
         .config("spark.sql.shuffle.partitions", str(PARTITIONS))
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS"))
    if udf_mode == "pandas":
        b = b.config("spark.sql.execution.arrow.pyspark.enabled", "true")
    return b.getOrCreate()


def read_one(spark, path):
    """Spec section 1: select 8 columns, cast to the canonical types, one file at a time."""
    df = spark.read.parquet(path).select(*KEEP)
    return df.select(
        F.date_trunc("second", F.col("tpep_pickup_datetime")).alias("tpep_pickup_datetime"),
        F.date_trunc("second", F.col("tpep_dropoff_datetime")).alias("tpep_dropoff_datetime"),
        F.col("passenger_count").cast("long").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("PULocationID").cast("int").alias("PULocationID"),
        F.col("DOLocationID").cast("int").alias("DOLocationID"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
    )


def ingest(spark, input_dir):
    files = sorted(glob.glob(os.path.join(input_dir, "*.parquet")))
    return reduce(DataFrame.unionByName, [read_one(spark, f) for f in files])


def cleanse(df):
    """Spec section 2, rules 1-8 in the stated order."""
    ts = lambda s: F.lit(s).cast("timestamp")
    pu, do = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    df = df.dropna()                                                     # 1 nulls
    df = df.filter((pu >= ts("2023-01-01 00:00:00")) & (pu < ts("2023-05-01 00:00:00")))   # 2
    df = df.filter(do > pu)                                              # 3
    df = df.filter((F.col("trip_distance") > 0) & (F.col("fare_amount") >= 0))             # 4
    df = df.filter(F.col("passenger_count").between(1, 8))               # 5
    df = df.filter(F.col("PULocationID").between(1, 263) & F.col("DOLocationID").between(1, 263))  # 6
    df = df.dropDuplicates()                                             # 7 exact duplicates
    return (df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))   # 8
              .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")))


def join_zones(spark, df, zones_path):
    """Spec section 3: Join A, explicit broadcast of the 265-row zone table."""
    z = (spark.read.csv(zones_path, header=True, inferSchema=True)
         .select(F.col("LocationID").cast("int").alias("LocationID"), "Borough", "Zone"))
    pu = F.broadcast(z.select(F.col("LocationID").alias("PULocationID"),
                              F.col("Borough").alias("pu_borough"), F.col("Zone").alias("pu_zone")))
    do = F.broadcast(z.select(F.col("LocationID").alias("DOLocationID"),
                              F.col("Borough").alias("do_borough"), F.col("Zone").alias("do_zone")))
    return df.join(pu, "PULocationID", "inner").join(do, "DOLocationID", "inner")


# ---- Spec section 4: speed_centi = floor(speed*100 + 0.5), speed = distance / (duration_sec / 3600) ----
@F.udf(LongType())
def speed_centi_py(dist, dur):
    if dist is None or dur is None or dur < 60:
        return None
    sp = dist / (dur / 3600.0)
    if sp > 100.0:
        return None
    return int(math.floor(sp * 100.0 + 0.5))


@F.pandas_udf(LongType())
def speed_centi_pd(dist: pd.Series, dur: pd.Series) -> pd.Series:
    sp = dist / (dur / 3600.0)
    ok = (dur >= 60) & (sp <= 100.0)
    return np.floor(sp * 100.0 + 0.5).where(ok).astype("Int64")


def apply_udf(df, mode):
    df = df.withColumn("duration_sec",
                       F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))
    if mode == "python":
        sc = speed_centi_py("trip_distance", "duration_sec")
    elif mode == "pandas":
        sc = speed_centi_pd("trip_distance", "duration_sec")
    else:  # native: same arithmetic as column expressions, no Python
        sp = F.col("trip_distance") / (F.col("duration_sec") / F.lit(3600.0))
        sc = F.when((F.col("duration_sec") >= 60) & (sp <= 100.0), F.floor(sp * 100.0 + 0.5))
    return (df.withColumn("speed_centi", sc)
              .withColumn("avg_speed_mph", F.col("speed_centi") / F.lit(100.0))
              .drop("duration_sec"))


def join_heavy(df):
    """Spec section 5: Join B, shuffle join (auto-broadcast is off)."""
    keys = ["PULocationID", "pickup_date", "pickup_hour"]
    agg = df.groupBy(*keys).agg(
        F.count(F.lit(1)).alias("zh_trip_count"),
        F.sum(F.floor(F.col("fare_amount") * 100.0 + 0.5)).alias("zh_fare_cents_sum"),
        F.coalesce(F.sum("speed_centi"), F.lit(0).cast("long")).alias("zh_speed_centi_sum"),
        F.count("speed_centi").alias("zh_speed_n"),
    )
    return df.join(agg, keys, "inner")


def checkpoint(df, instrument):
    """Instrumented runs save each stage's result and count it, so the stage time is real."""
    if not instrument:
        return df, None
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    return df, df.count()


OUT_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
            "PULocationID", "DOLocationID", "fare_amount", "total_amount", "pickup_date", "pickup_hour",
            "pu_borough", "pu_zone", "do_borough", "do_zone", "avg_speed_mph", "speed_centi",
            "zh_trip_count", "zh_fare_cents_sum", "zh_speed_centi_sum", "zh_speed_n"]


def export(df, out_dir):
    """Spec section 6: final columns in order, snappy Parquet, no sort inside the pipeline."""
    (df.select(*OUT_COLS).write.mode("overwrite")
       .option("compression", "snappy").parquet(out_dir))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data")
    ap.add_argument("--zones", default="data/taxi_zone_lookup.csv")
    ap.add_argument("--output", default="out/spark")
    ap.add_argument("--udf-mode", default="python", choices=["python", "pandas", "native"])
    ap.add_argument("--run-id", default="test")
    ap.add_argument("--instrument", action="store_true",
                    help="materialise and count after every stage (per-stage timings)")
    ap.add_argument("--hold", type=int, default=0, help="seconds to keep the Spark UI alive after the run (for screenshots)")
    args = ap.parse_args()

    spark = build_session(args.udf_mode)
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext
    print("master:", sc.master, "| app:", sc.applicationId, "| udf:", args.udf_mode, "| instrument:", args.instrument, flush=True)
    assert sc.master.startswith("spark://"), "not running on the standalone cluster"

    timer = StageTimer("spark", args.udf_mode, args.run_id + ("_inst" if args.instrument else "_e2e"))
    timer.start()

    prev = None
    def step(name, build):
        nonlocal prev
        with timer.stage(name):
            df = build()
            df, n = checkpoint(df, args.instrument)
        if n is not None:
            timer.count("after_" + name, n)
            print("rows after %-12s %d" % (name, n), flush=True)
        if prev is not None and args.instrument:
            prev.unpersist()
        prev = df
        return df

    df = step("ingest", lambda: ingest(spark, args.input))
    df = step("cleanse", lambda: cleanse(df))
    df = step("join_zones", lambda: join_zones(spark, df, args.zones))
    df = step("apply_udf", lambda: apply_udf(df, args.udf_mode))
    df = step("join_heavy", lambda: join_heavy(df))

    plan = df._jdf.queryExecution().executedPlan().toString()
    os.makedirs("results", exist_ok=True)
    with open("results/spark_plan_%s.txt" % args.udf_mode, "w") as f:
        f.write(plan)
    print("BroadcastHashJoin (Join A) in plan:", "BroadcastHashJoin" in plan,
          "| SortMergeJoin (Join B) in plan:", "SortMergeJoin" in plan, flush=True)

    with timer.stage("export"):
        export(df, args.output)

    timer.save(extra={"config": {"master": sc.master, "shuffle_partitions": PARTITIONS,
                                  "aqe": False, "spark_version": spark.version,
                                  "instrument": args.instrument}})
    if args.hold:
        print("holding the Spark UI open for %d s: http://127.0.0.1:4040" % args.hold, flush=True)
        import time; time.sleep(args.hold)
    spark.stop()


if __name__ == "__main__":
    main()
