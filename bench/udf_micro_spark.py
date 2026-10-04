import json, math, os, sys, time
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
import numpy as np
import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType

SRC = "out/ray"
COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"]
MODES = ["python", "pandas", "native"]


@F.udf(LongType())
def speed_py(dist, dur):
    if dist is None or dur is None or dur < 60:
        return None
    sp = dist / (dur / 3600.0)
    if sp > 100.0:
        return None
    return int(math.floor(sp * 100.0 + 0.5))


@F.pandas_udf(LongType())
def speed_pd(dist: pd.Series, dur: pd.Series) -> pd.Series:
    sp = dist / (dur / 3600.0)
    ok = (dur >= 60) & (sp <= 100.0)
    return np.floor(sp * 100.0 + 0.5).where(ok).astype("Int64")


def with_speed(df, mode):
    df = df.withColumn("duration_sec", F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))
    if mode == "python":
        c = speed_py("trip_distance", "duration_sec")
    elif mode == "pandas":
        c = speed_pd("trip_distance", "duration_sec")
    else:
        sp = F.col("trip_distance") / (F.col("duration_sec") / F.lit(3600.0))
        c = F.when((F.col("duration_sec") >= 60) & (sp <= 100.0), F.floor(sp * 100.0 + 0.5))
    return df.withColumn("speed_centi", c)


spark = (SparkSession.builder.master("spark://127.0.0.1:7077").appName("a3-udf-micro")
         .config("spark.driver.host", "127.0.0.1").config("spark.driver.memory", "1g")
         .config("spark.executor.memory", "1g").config("spark.executor.cores", "2").config("spark.cores.max", "4")
         .config("spark.sql.shuffle.partitions", "8").config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
assert spark.sparkContext.master.startswith("spark://")
df = spark.read.parquet(SRC).select(*COLS).repartition(4).persist(StorageLevel.MEMORY_AND_DISK)
n = df.count()
print("spark rows:", n, flush=True)
for mode in MODES:
    with_speed(df.limit(200000), mode).agg(F.sum("speed_centi")).collect()
res = {"framework": "spark", "rows": n, "runs": {}, "checksum": {}}
for rep in range(3):
    k = rep % 3
    for mode in MODES[k:] + MODES[:k]:
        t0 = time.perf_counter()
        total = with_speed(df, mode).agg(F.sum("speed_centi")).collect()[0][0]
        dt = time.perf_counter() - t0
        res["runs"].setdefault(mode, []).append(round(dt, 3))
        res["checksum"][mode] = int(total)
        print("spark %-7s rep %d %6.1fs sum=%s" % (mode, rep + 1, dt, total), flush=True)
json.dump(res, open("results/udf_micro_spark.json", "w"), indent=1)
spark.stop()
