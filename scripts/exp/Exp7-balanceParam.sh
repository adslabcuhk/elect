#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
# Exp7: YCSB core workloads, 3-way replication, (6,4) encoding, vary target storage saving, 10M KV + 1M OP.

ExpName="Exp7-balanceParam"
schemes=("elect")
workloads=("workloadRead" "workloadWrite")
runningTypes=("normal" "degraded")
KVNumber=10000000
keyLength=24
valueLength=1000
operationNumber=1000000
simulatedClientNumber=${defaultSimulatedClientNumber}
RunningRoundNumber=1
storageSavingTargetSet=(0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9)

# Setup hosts
setupNodeInfo ./hosts.ini
# Run Exp
for scheme in "${schemes[@]}"; do
    for storageSavingTarget in "${storageSavingTargetSet[@]}"; do
        echo "Start experiment of ${scheme} with storage saving target=${storageSavingTarget}"
        # Load data for evaluation
        loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${simulatedClientNumber}" "${storageSavingTarget}"

        # Run experiment
        for workload in "${workloads[@]}"; do
            for runningMode in "${runningTypes[@]}"; do
                # Run experiment
                doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${operationNumber}" "${simulatedClientNumber}" "${RunningRoundNumber}" "${runningMode}" "${workload}" "ONE" "savingTarget=${storageSavingTarget}"
            done
        done
    done
done

# Generate the summarized results
if [ -f "${PathToScripts}/exp/${ExpName}.log" ]; then
    rm -rf "${PathToScripts}/exp/${ExpName}.log"
fi

for scheme in "${schemes[@]}"; do
    for storageSavingTarget in "${storageSavingTargetSet[@]}"; do
        echo "Storage usage of ${scheme} under the erasure coding params k = $erasureCodingK" >>${PathToScripts}/exp/${ExpName}.log
        ${PathToScripts}/count/fetchStorage.sh "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "4" "${storageSavingTarget}" >>${PathToScripts}/exp/${ExpName}.log
    done
done

for scheme in "${schemes[@]}"; do
    ${PathToScripts}/count/fetchPerformance.sh all "${ExpName}" "${scheme}" >>${PathToScripts}/exp/${ExpName}.log
done

cat ${PathToScripts}/exp/${ExpName}.log
