#!/bin/bash
. /etc/profile

ExpName="Exp4"
SysName="elect"
prefixPath="/home/tinoryj/FinalResults/Exp4-op/${SysName}"
nodeList=(1 2 3 4 5 6 7 8 9 10)
workloads=("workloadRead" "workloadWrite" "workloadScan" "workloadUpdate")
roundNumber=5

mkdir -p ./RAM/Load
mkdir -p ./RAM/Compact
for node in "${nodeList[@]}"; do
    cp -r $prefixPath/$ExpName-Load-Node$node/${ExpName}-${SysName}-Load_Loading_memory_usage.txt ./RAM/Load/node${node}-RAM.txt
    cp -r $prefixPath/$ExpName-Compaction-Node$node/${ExpName}-${SysName}-Load_Compaction_memory_usage.txt ./RAM/Compact/node${node}-RAM.txt
done

for ((round = 1; round <= roundNumber; round++)); do
    mkdir -p ./RAM/round-$round/normal
    mkdir -p ./RAM/round-$round/degraded
    for workload in "${workloads[@]}"; do
        echo "Merge RAM usage round $round for workload $workload"
        mkdir -p ./RAM/round-$round/normal/$workload
        mkdir -p ./RAM/round-$round/degraded/$workload
        for node in "${nodeList[@]}"; do
            cp -r $prefixPath/$ExpName-${workload}-$round-Node$node/${ExpName}-${SysName}-Run-normal-Round-${round}_Running_memory_usage.txt ./RAM/round-$round/normal/$workload/node${node}-round${round}-RAM.txt
        done
        for node in "${nodeList[@]}"; do
            cp -r $prefixPath/$ExpName-${workload}-$round-Node$node/Results/${ExpName}-${SysName}-Run-degraded-Round-${round}_Running_memory_usage.txt ./RAM/round-$round/degraded/$workload/node${node}-round${round}-RAM.txt
        done
    done
done
