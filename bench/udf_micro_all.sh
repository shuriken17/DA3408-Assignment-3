#!/bin/bash
cd ~/Assignment_3
source ~/miniforge3/etc/profile.d/conda.sh
conda activate a3
export RAY_AUTH_MODE=disabled
export SPARK_HOME=$HOME/Assignment_3/spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
log() { echo "[$(date +%H:%M:%S)] $*"; }
[ -d out/ray ] || { log "ABORT: out/ray is missing"; exit 1; }
ray stop >/dev/null 2>&1
$SPARK_HOME/sbin/stop-worker.sh >/dev/null 2>&1
$SPARK_HOME/sbin/stop-master.sh >/dev/null 2>&1
sleep 3
log "starting Spark"
$SPARK_HOME/sbin/start-master.sh >/dev/null && sleep 5
$SPARK_HOME/sbin/start-worker.sh spark://127.0.0.1:7077 >/dev/null && sleep 8
N=$(curl -s http://127.0.0.1:8080/json/ | python -c "import sys,json; print(len([w for w in json.load(sys.stdin)['workers'] if w['state']=='ALIVE']))")
log "spark workers alive: $N"
[ "$N" = "2" ] || { log "ABORT: Spark cluster not ready"; exit 1; }
python -u bench/udf_micro_spark.py || { log "ABORT: spark micro failed"; exit 1; }
$SPARK_HOME/sbin/stop-worker.sh >/dev/null 2>&1
$SPARK_HOME/sbin/stop-master.sh >/dev/null 2>&1
sleep 5
log "starting Ray"
IP=$(hostname -I | awk '{print $1}')
ray start --head --disable-usage-stats --node-ip-address=127.0.0.1 --port=6379 --num-cpus=2 --memory=1073741824 --object-store-memory=524288000 --dashboard-host=127.0.0.1 --dashboard-port=8265 >/dev/null 2>&1
ray start --address=$IP:6379 --node-ip-address=$IP --num-cpus=2 --memory=1073741824 --object-store-memory=524288000 --node-manager-port=6390 --object-manager-port=6391 --min-worker-port=20000 --max-worker-port=20199 --dashboard-agent-listen-port=52370 --dashboard-agent-grpc-port=52371 --runtime-env-agent-port=52372 --metrics-export-port=8090 >/dev/null 2>&1
sleep 8
python -u bench/udf_micro_ray.py || { log "ABORT: ray micro failed"; exit 1; }
ray stop >/dev/null 2>&1
log "UDF MICRO DONE"
