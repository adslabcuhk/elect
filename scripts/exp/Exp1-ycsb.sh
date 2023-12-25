#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

# Exp1: YCSB core workloads, 3-way replication, (6,4) encoding, 60% target storage saving, 100M KV + 10M OP.

ExpName="Exp1-ycsb"
schemes=("cassandra" "elect")
workloads=("workloada" "workloadb" "workloadc" "workloadd" "workloade" "workloadf")
runningTypes=("normal")
KVNumber=100000000
keyLength=24
valueLength=1000
operationNumber=1000000
simulatedClientNumber=${defaultSimulatedClientNumber}
RunningRoundNumber=1

# Setup hosts
setupNodeInfo hosts.ini

# Run Exp
for scheme in "${schemes[@]}"; do
    echo "Start experiment of ${scheme} (Loading)"
    # Load data for evaluation
    loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${simulatedClientNumber}"

    for workload in "${workloads[@]}"; do
        for runningMode in "${runningTypes[@]}"; do
            # Run experiment
            doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${operationNumber}" "${simulatedClientNumber}" "${RunningRoundNumber}" "${runningMode}" "${workload}" "ONE"
        done
    done
done

# Generate the summarized results
if [ -f "${PathToScripts}/exp/${ExpName}.log" ]; then
    rm -rf "${PathToScripts}/exp/${ExpName}.log"
fi
for scheme in "${schemes[@]}"; do
    echo "Storage usage of ${scheme}" >>${PathToScripts}/exp/${ExpName}.log
    ${PathToScripts}/count/fetchStorage.sh "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" >>${PathToScripts}/exp/${ExpName}.log
done

for scheme in "${schemes[@]}"; do
    ${PathToScripts}/count/fetchPerformance.sh all "${ExpName}" "${scheme}" >>${PathToScripts}/exp/${ExpName}.log
done

cat ${PathToScripts}/exp/${ExpName}.log
