#!/bin/bash
. /etc/profile

ExpName="Exp4"
SysName="elect"
prefixPath="/home/tinoryj/FinalResults/Exp4-op/${SysName}"
nodeList=(1 2 3 4 5 6 7 8 9 10)
workloads=("workloadRead" "workloadWrite" "workloadScan" "workloadUpdate")
roundNumber=5

mkdir -p ./CPU/Load
mkdir -p ./CPU/Compact
for node in "${nodeList[@]}"; do
    cp -r $prefixPath/$ExpName-Load-Node$node/${ExpName}-${SysName}-Load_Loading_cpu_usage.txt ./CPU/Load/node${node}-CPU.txt
    cp -r $prefixPath/$ExpName-Compaction-Node$node/${ExpName}-${SysName}-Load_Compaction_cpu_usage.txt ./CPU/Compact/node${node}-CPU.txt
done

for ((round = 1; round <= roundNumber; round++)); do
    mkdir -p ./CPU/round-$round/normal
    mkdir -p ./CPU/round-$round/degraded
    for workload in "${workloads[@]}"; do
        echo "Merge CPU usage round $round for workload $workload"
        mkdir -p ./CPU/round-$round/normal/$workload
        mkdir -p ./CPU/round-$round/degraded/$workload
        for node in "${nodeList[@]}"; do
            cp -r $prefixPath/$ExpName-${workload}-$round-Node$node/${ExpName}-${SysName}-Run-normal-Round-${round}_Running_cpu_usage.txt ./CPU/round-$round/normal/$workload/node${node}-round${round}-CPU.txt
        done
        for node in "${nodeList[@]}"; do
            cp -r $prefixPath/$ExpName-${workload}-$round-Node$node/Results/${ExpName}-${SysName}-Run-degraded-Round-${round}_Running_cpu_usage.txt ./CPU/round-$round/degraded/$workload/node${node}-round${round}-CPU.txt
        done
    done
done
