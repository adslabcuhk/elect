#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
# Exp2: YCSB core workloads, 3-way replication, (6,4) encoding, 60% target storage saving, 10M KV + 1M OP.

ExpName="test"
schemes=("elect" "cassandra")
workloads=("workloadRead" "workloadWrite" "workloadScan" "workloadUpdate")
runningTypes=("normal" "degraded")
KVNumber=10000000
keyLength=24
valueLength=1000
operationNumber=1000000
simulatedClientNumber=${defaultSimulatedClientNumber}
RunningRoundNumber=1

# Setup hosts
setupNodeInfo ./hosts.ini
# Run Exp
for scheme in "${schemes[@]}"; do
    echo "Start experiment of ${scheme}"
    # Load data for evaluation
    loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${simulatedClientNumber}"

    # Run experiment
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
# output resource usage
outputTypeSet=("Load" "normal" "degraded")
for scheme in "${schemes[@]}"; do
    for outputType in "${outputTypeSet[@]}"; do
        ${PathToScripts}/count/fetchResource.sh "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${operationNumber}" "${simulatedClientNumber}" "${outputType}" "ONE" "4" "0.6" "${workloads[@]}" >>${PathToScripts}/exp/${ExpName}.log
    done
done
# output storage usage
for scheme in "${schemes[@]}"; do
    echo "Storage usage of ${scheme}" >>${PathToScripts}/exp/${ExpName}.log
    ${PathToScripts}/count/fetchStorage.sh "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" >>${PathToScripts}/exp/${ExpName}.log
done
# output performance
for scheme in "${schemes[@]}"; do
    ${PathToScripts}/count/fetchPerformance.sh all "${ExpName}" "${scheme}" >>${PathToScripts}/exp/${ExpName}.log
done

cat ${PathToScripts}/exp/${ExpName}.log
