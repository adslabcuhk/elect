#!/bin/bash
. /etc/profile

types=("normal" "degraded")
roundNumber=5
# workloads=("workloadRead")
workloads=("workloadRead" "workloadWrite" "workloadScan" "workloadUpdate")

echo "The RAM usage of of Load"
python3 countRAM.py ./RAM/Load/

echo "The RAM usage of of Compaction"
python3 countRAM.py ./RAM/Compact/

for type in "${types[@]}"; do
    for workload in "${workloads[@]}"; do
        for ((round = 1; round <= roundNumber; round++)); do
            echo "The RAM usage of workload ${workload} in round ${round} with type ${type}"
            python3 countRAM.py ./RAM/round-${round}/${type}/${workload}/
        done
    done
done
