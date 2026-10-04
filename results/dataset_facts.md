# Dataset facts (measured on the VM, 4 cores, ~5.8 GB RAM)
- Source: NYC TLC yellow taxi trips, 2023-01 to 2023-04 (4 Parquet files) + taxi_zone_lookup.csv (265 rows)
- Raw rows: 12,672,737 (Jan 3,066,766 | Feb 2,913,955 | Mar 3,403,766 | Apr 3,288,250)
- On disk: 197 MB. Decompressed in Arrow memory: 1.84 GB (all columns)
- Type differences: Jan passenger_count double / location IDs int64; Feb-Apr int64 / int32; Airport_fee vs airport_fee
- NaN and inf in kept columns: 0 (checked with pyarrow and NumPy). Nulls: passenger_count only, 326,869 rows
- Defect counts per file (Jan/Feb/Mar/Apr):
  dropoff<=pickup 1121/1265/1453/1172; pickup outside Jan-Apr 2023 38/5/11/54;
  distance<=0 45862/41098/48528/40234; fare<0 25049/24764/29677/29988;
  passengers 0 or >8 51165/47283/58368/56956; zone 264/265 58248/53430/53424/54728
