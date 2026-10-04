# Pipeline Specification (shared by spark_clean.py and ray_clean.py)

## 0. Run parameters
- Input: data/ (4 Parquet files, 2023-01 to 2023-04) + data/taxi_zone_lookup.csv
- Partitions/blocks: 8 in both frameworks. Timezone: UTC. Timestamps are naive.

## 1. Ingest
The four Parquet files have different column types (Jan: passenger_count double, location IDs int64;
Feb-Apr: passenger_count int64, location IDs int32), so they are NOT read as one dataset.
For EACH file, in this order:
  1. Select only these columns (airport_fee/Airport_fee is dropped):
     tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
     PULocationID, DOLocationID, fare_amount, total_amount
  2. Cast to the canonical types: both timestamps = timestamp truncated to whole seconds;
     passenger_count = int64; PULocationID, DOLocationID = int32;
     trip_distance, fare_amount, total_amount = float64.
Then union the four results by column name.
Verified on the data: no NaN values in any kept column, and Jan passenger_count has no fractional values,
so the double -> int64 cast is lossless. Nulls (passenger_count only, 326,869 rows) are removed in step 2.1.
The zone lookup is read from data/taxi_zone_lookup.csv.

## 2. Cleanse (applied in this order)
1. Drop rows with a null in any kept column.
2. Keep pickup >= 2023-01-01 and pickup < 2023-05-01.
3. Keep dropoff > pickup.
4. Keep trip_distance > 0 and fare_amount >= 0.
5. Keep passenger_count between 1 and 8 inclusive.
6. Keep PULocationID and DOLocationID between 1 and 263 inclusive (264 and 265 mean unknown).
7. Remove exact duplicates across all kept columns (identical rows, so which copy survives is irrelevant).
8. Derive pickup_date (date) and pickup_hour (int, 0-23) from pickup.

## 3. Join A (dimension join, broadcast in both frameworks)
Inner join taxi_zone_lookup on PULocationID -> pu_borough, pu_zone;
inner join again on DOLocationID -> do_borough, do_zone.
Lookup columns: LocationID, Borough, Zone (service_zone is dropped).

## 4. UDF: avg_speed_mph (Python function in both frameworks)
duration_sec = whole seconds between dropoff and pickup (both already whole seconds)
speed = trip_distance / (duration_sec / 3600.0)
- duration_sec < 60 -> null
- speed > 100 -> null
- else avg_speed_mph = floor(speed * 100 + 0.5) / 100   (explicit half-up rounding to 2 decimals;
  never use the language's built-in round())
Also compute speed_centi = floor(speed * 100 + 0.5) as int64 (null when avg_speed_mph is null).

## 5. Join B (heavy shuffle join, NOT broadcast; Spark autoBroadcastJoinThreshold = -1)
Group trips by (PULocationID, pickup_date, pickup_hour) and compute:
- zh_trip_count      = count of rows (int64)
- zh_fare_cents_sum  = sum of floor(fare_amount * 100 + 0.5) as int64
- zh_speed_centi_sum = sum of speed_centi, nulls ignored (int64)
- zh_speed_n         = count of non-null speed_centi (int64)
Inner join this table back onto the trips on those three keys.

- A group with no non-null speed_centi has zh_speed_centi_sum = 0 (never null).
- Join B reads the post-UDF data twice. No caching in end-to-end runs in either framework (the UDF is evaluated twice).

## 6. Export
Write Parquet, snappy compression, to out/spark or out/ray. No sort inside the pipeline.
Final columns, in this order:
tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
PULocationID, DOLocationID, fare_amount, total_amount, pickup_date, pickup_hour,
pu_borough, pu_zone, do_borough, do_zone, avg_speed_mph, speed_centi,
zh_trip_count, zh_fare_cents_sum, zh_speed_centi_sum, zh_speed_n

## 7. Parity check (verify_parity.py)
Row counts equal, schemas equal, both outputs sorted on all columns then content-hashed, per-column diff if they differ.
