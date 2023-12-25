#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
# Exp6: YCSB core workloads, 3-way replication, (6,4) encoding, 60% target storage saving, 10M KV + 1M OP, vary KV sizes.

ExpName="Exp6-kvSize"
schemes=("cassandra" "elect")
workloads=("workloadRead")
runningTypes=("normal" "degraded")
dataSize=10 # 10 GiB
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
        KVNumber=$(echo "1073741824 * ${dataSize} / (${keyLength} + ${fixedFieldlength})" | bc -l)
        KVNumber=$(echo "scale=0; (${KVNumber} + 0.5)/1" | bc)
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
        KVNumber=$(echo "1073741824 * ${dataSize} / (${fixedKeylength} + ${valueLength})" | bc -l)
        KVNumber=$(echo "scale=0; (${KVNumber} + 0.5)/1" | bc)
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
if [ -f "${PathToScripts}/exp/${ExpName}.log" ]; then
    rm -rf "${PathToScripts}/exp/${ExpName}.log"
fi

for scheme in "${schemes[@]}"; do
    for keyLength in "${keyLengthSet[@]}"; do
        KVNumber=$(echo "1073741824 * ${dataSize} / (${keyLength} + ${fixedFieldlength})" | bc -l)
        KVNumber=$(echo "scale=0; (${KVNumber} + 0.5)/1" | bc)
        ${PathToScripts}/count/fetchStorage.sh "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${fixedFieldlength}" >>${PathToScripts}/exp/${ExpName}.log
    done
    for valueLength in "${valueLengthSet[@]}"; do
        KVNumber=$(echo "1073741824 * ${dataSize} / (${fixedKeylength} + ${valueLength})" | bc -l)
        KVNumber=$(echo "scale=0; (${KVNumber} + 0.5)/1" | bc)
        ${PathToScripts}/count/fetchStorage.sh "${ExpName}" "${scheme}" "${KVNumber}" "${fixedKeylength}" "${valueLength}" >>${PathToScripts}/exp/${ExpName}.log
    done
done

for scheme in "${schemes[@]}"; do
    ${PathToScripts}/count/fetchPerformance.sh all "${ExpName}" "${scheme}" >>${PathToScripts}/exp/${ExpName}.log
done

cat ${PathToScripts}/exp/${ExpName}.log
