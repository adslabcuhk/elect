#!/bin/bash
. /etc/profile

types=("normal" "degraded")
roundNumber=5
# workloads=("workloadRead")
workloads=("workloadRead" "workloadWrite" "workloadScan" "workloadUpdate")

echo "The CPU usage of of Load"
python3 countCPU.py ./CPU/Load/

echo "The CPU usage of of Compaction"
python3 countCPU.py ./CPU/Compact/


for type in "${types[@]}"; do
    for workload in "${workloads[@]}"; do
        for ((round = 1; round <= roundNumber; round++)); do
            echo "The CPU usage of workload ${workload} in round ${round} with type ${type}"
            python3 countCPU.py ./CPU/round-${round}/${type}/${workload}/
        done
    done
done
