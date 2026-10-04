#!/bin/bash
cd ~/Assignment_3
source ~/miniforge3/etc/profile.d/conda.sh
conda activate a3
export RAY_AUTH_MODE=disabled
log() { echo "[$(date +%H:%M:%S)] $*"; }
ray stop >/dev/null 2>&1
sleep 5
log "starting Ray"
IP=$(hostname -I | awk '{print $1}')
ray start --head --disable-usage-stats --node-ip-address=127.0.0.1 --port=6379 --num-cpus=2 --memory=1073741824 --object-store-memory=524288000 --dashboard-host=127.0.0.1 --dashboard-port=8265 >/dev/null 2>&1
ray start --address=$IP:6379 --node-ip-address=$IP --num-cpus=2 --memory=1073741824 --object-store-memory=524288000 --node-manager-port=6390 --object-manager-port=6391 --min-worker-port=20000 --max-worker-port=20199 --dashboard-agent-listen-port=52370 --dashboard-agent-grpc-port=52371 --runtime-env-agent-port=52372 --metrics-export-port=8090 >/dev/null 2>&1
sleep 8
timeout 1500 python -u bench/udf_micro_ray.py || { log "ABORT: ray micro failed or timed out"; ray stop >/dev/null 2>&1; exit 1; }
ray stop >/dev/null 2>&1
log "UDF MICRO DONE"
