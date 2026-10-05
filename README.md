# DA3408 AI Operations — Assignment 3

**Prabhav Gupta DA24B018**

This repo has my work for Assignment 3: the same data cleaning pipeline built in Apache Spark and in Ray Data, run on a 2-worker cluster for each, and compared.

A report with the benchmark graphs and the conclusion is in `DA3408_Assignment_3_Report.pdf` at the root of this repo.

## Requirements

- Linux machine with at least 4 cores and about 6 GB RAM
- Java 17, Python 3.11, Spark 3.5.3 standalone binaries, Ray 2.59
- Data: NYC yellow taxi trips, Jan–Apr 2023 (4 Parquet files, 12,672,737 rows) and `taxi_zone_lookup.csv`

The scripts assume 2 Spark workers and 2 Ray nodes with 2 cores each (4 cores in total), a Spark master at `spark://127.0.0.1:7077`, and the repo cloned to `~/Assignment_3` (the benchmark scripts use that path).

## Repository structure

```
spark_clean.py        Spark pipeline (--udf-mode python | pandas | native)
ray_clean.py          Ray Data pipeline (same three modes)
verify_parity.py      checks that two output folders are identical
requirements.txt
common/
  pipeline_spec.md    the exact cleaning rules both scripts follow
  timing.py           stage timer used by both scripts
  monitor.py          samples CPU and memory every second
bench/
  run_bench.py        runs each mode 3 times with monitoring
  all.sh              starts each cluster and runs the full benchmark
  udf_micro_*         times only the UDF stage on cached data
  make_charts.py      builds the tables and charts
charts/               benchmark figures used in the report
screenshots/          Spark Master UI (2 workers), Spark UI 4040, Ray Dashboard (2 nodes)
results/              per-run JSON timings, monitor CSVs, parity outputs, benchmark table, logs
```

## Pipeline

1. Ingest each Parquet file, keep 8 columns, cast to fixed types, union.
2. Clean: nulls, date range, dropoff after pickup, distance and fare, passengers 1–8, zones 1–263, exact duplicates (12,672,737 → 11,698,059 rows).
3. Join A: broadcast join with the zone table. Then the average speed (mph) UDF.
4. Join B: per (pickup zone, date, hour) totals joined back to the trips (a shuffle join in Spark).
5. Write Parquet with 20 columns.

The full rules are in `common/pipeline_spec.md`.

## Install

```bash
git clone https://github.com/shuriken17/DA3408-Assignment-3.git ~/Assignment_3
cd ~/Assignment_3

sudo apt update && sudo apt install -y openjdk-17-jdk wget
conda create -n a3 python=3.11 -y && conda activate a3
pip install -r requirements.txt

wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
tar -xzf spark-3.5.3-bin-hadoop3.tgz && mv spark-3.5.3-bin-hadoop3 spark && rm spark-3.5.3-bin-hadoop3.tgz
export SPARK_HOME=$PWD/spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

mkdir -p data && cd data
for m in 01 02 03 04; do wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-$m.parquet; done
wget -O taxi_zone_lookup.csv "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
cd ..
```

## Run the pipelines

Run one framework at a time (4 cores, about 6 GB RAM).

**Spark** (2 workers, 2 cores and 1 GB each):

```bash
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
cat >> $SPARK_HOME/conf/spark-env.sh <<'EOF'
export SPARK_MASTER_HOST=127.0.0.1
export SPARK_WORKER_INSTANCES=2
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=1g
EOF

$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://127.0.0.1:7077     # Master UI: http://127.0.0.1:8080 shows 2 workers
python spark_clean.py --udf-mode python --run-id x --output out/spark
python spark_clean.py --instrument --udf-mode python --run-id udftest    # optional: per-stage timings for the stage chart
$SPARK_HOME/sbin/stop-worker.sh && $SPARK_HOME/sbin/stop-master.sh
```

**Ray** (head plus one worker node on the same machine, 2 CPUs and 1 GB each):

```bash
export RAY_AUTH_MODE=disabled
VM_IP=$(hostname -I | awk '{print $1}')
ray start --head --disable-usage-stats --node-ip-address=127.0.0.1 --port=6379 --num-cpus=2 --memory=1073741824 --object-store-memory=524288000 --dashboard-host=127.0.0.1 --dashboard-port=8265
ray start --address=$VM_IP:6379 --node-ip-address=$VM_IP --num-cpus=2 --memory=1073741824 --object-store-memory=524288000 --node-manager-port=6390 --object-manager-port=6391 --min-worker-port=20000 --max-worker-port=20199 --dashboard-agent-listen-port=52370 --dashboard-agent-grpc-port=52371 --runtime-env-agent-port=52372 --metrics-export-port=8090
ray status                                                   # 2 nodes, 4 CPUs; Dashboard: http://127.0.0.1:8265
python ray_clean.py --udf-mode python --run-id x --output out/ray
python ray_clean.py --instrument --udf-mode python --run-id full         # optional: per-stage timings for the stage chart
ray stop
```

**Check that the outputs are identical:**

```bash
python verify_parity.py out/spark out/ray                    # expect PARITY PASS
```

## Benchmark and charts

```bash
bash bench/all.sh                  # starts and stops each cluster itself: 18 runs (2 frameworks x 3 modes x 3 repeats), with CPU/memory monitoring
bash bench/udf_micro_all.sh        # UDF-only timing on cached data (needs out/ray from the Ray run above)
python bench/make_charts.py        # tables in results/, figures in charts/
```

## What I did

1. Checked the data (types, nulls, defects) and wrote one shared specification for the cleaning rules, so both scripts do the same thing.
2. Set up the Spark cluster (master and 2 workers) and the Ray cluster (head and 1 worker node), and checked each with a test job and its UI.
3. Wrote `spark_clean.py` and `ray_clean.py` and ran each with three UDF modes: row-by-row Python, pandas (Arrow batches) and native built-in functions.
4. Compared outputs with `verify_parity.py` (schema, row count, column checksums and a checksum of the full sorted row set). All Spark and Ray modes matched at 11,698,059 rows. Timestamps are compared in microseconds because Spark and Ray wrote different precision.
5. Benchmarked all six combinations three times each, sampling CPU and memory every second (`psutil` and `top`).
6. Timed only the UDF stage on cached data to measure the Python UDF overhead in each framework.
7. Made the charts and wrote the report.

Main result: Spark was faster in every mode (median 98.6–126.5 s against 246.9–381.5 s for Ray). Ray's Python UDF overhead was smaller (+1.3 s against +8.9 s for Spark).

## Notes specific to my runs

- Environment: Ubuntu VM in VirtualBox, 4 cores, 5.8 GB RAM, Ray and Spark on the same VM.
- Ray Data has no distinct operation, and its group-by shuffle did not fit in memory here, so Ray's duplicate removal and Join B run in 8 hash buckets, each reading the input again. Ray therefore does different work from Spark's single shuffle.
- `systemd-oomd` killed runs under memory pressure on this VM, so it was stopped during the benchmark and started again afterwards (`sudo systemctl stop systemd-oomd.service systemd-oomd.socket`). This is not needed on a machine with more memory.
- One VM, three runs per mode and no page-cache clearing, so the numbers are indicative only.

## AI usage

Tool used: Claude. Used for help translating logic between PySpark and Ray Data, debugging, tuning suggestions, and drafting this README and the report. I ran the cluster setup and all experiments in my own terminal and checked them in the Spark and Ray UIs. The tuning suggestions I adopted are listed in the report (Part 6). All benchmark numbers come from my own runs.

This repo is private until the deadline and will be made public after, as asked.
