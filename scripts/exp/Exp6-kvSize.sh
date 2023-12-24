#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
# Exp6: YCSB core workloads, 3-way replication, (6,4) encoding, 60% target storage saving, 10M KV + 1M OP, vary KV sizes.

ExpName="Exp6-kvSize"
schemes=("cassandra" "elect")
workloads=("workloadRead")
runningTypes=("normal" "degraded")
KVNumber=10000000
keyLengthSet=(8 16 32 64 128)
valueLengthSet=(32 128 512 2048 8192)
fixedKeylength=32
fixedFieldlength=512
operationNumber=1000000
simulatedClientNumber=${defaultSimulatedClientNumber}
RunningRoundNumber=1

# Setup hosts
setupNodeInfo ./hosts.ini
# Run Exp
for scheme in "${schemes[@]}"; do
    echo "Start experiment of ${scheme}"
    for keyLength in "${keyLengthSet[@]}"; do
        # Load data for evaluation
        loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${fixedFieldlength}" "${simulatedClientNumber}"

        # Run experiment
        for workload in "${workloads[@]}"; do
            for runningMode in "${runningTypes[@]}"; do
                # Run experiment
                doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${fixedFieldlength}" "${operationNumber}" "${simulatedClientNumber}" "${RunningRoundNumber}" "${runningMode}" "${workload}" "ONE"
            done
        done
    done

    for valueLength in "${valueLengthSet[@]}"; do
        # Load data for evaluation
        loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${fixedKeylength}" "${valueLength}" "${simulatedClientNumber}"

        # Run experiment
        for workload in "${workloads[@]}"; do
            for runningMode in "${runningTypes[@]}"; do
                # Run experiment
                doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${fixedKeylength}" "${valueLength}" "${operationNumber}" "${simulatedClientNumber}" "${RunningRoundNumber}" "${runningMode}" "${workload}" "ONE"
            done
        done
    done
done

# Generate the summarized results
for scheme in "${schemes[@]}"; do
    for keyLength in "${keyLengthSet[@]}"; do
        echo "Storage usage of ${scheme} under key size = ${keyLength} and value size = ${fixedFieldlength}" >>${PathToScripts}/exp/${ExpName}.log
        ${PathToScripts}/count/fetchStorage.sh "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${fixedFieldlength}" >>${PathToScripts}/exp/${ExpName}.log
    done
    for valueLength in "${valueLengthSet[@]}"; do
        echo "Storage usage of ${scheme} under key size = ${fixedKeylength} and value size = ${valueLength}" >>${PathToScripts}/exp/${ExpName}.log
        ${PathToScripts}/count/fetchStorage.sh "${ExpName}" "${scheme}" "${KVNumber}" "${fixedKeylength}" "${valueLength}" >>${PathToScripts}/exp/${ExpName}.log
    done
done

for scheme in "${schemes[@]}"; do
    ${PathToScripts}/count/fetchPerformance.sh all "${ExpName}" "${scheme}" >>${PathToScripts}/exp/${ExpName}.log
done

cat ${PathToScripts}/exp/${ExpName}.log
