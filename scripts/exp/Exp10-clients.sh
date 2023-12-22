#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
# Exp10: YCSB core workloads, 3-way replication, (6,4) encoding, 60% target storage saving, 10M KV. Varying number of clients, each client issues 100K operations.

ExpName="Exp10-clients"
schemes=("cassandra" "elect")
workloads=("workloadRead" "workloadWrite")
runningTypes=("normal")
KVNumber=10000000
keyLength=24
valueLength=1000
operationNumber=1000000
simulatedClientNumberSet=(8 16 32 64 128 256)
RunningRoundNumber=1

# Setup hosts
setupNodeInfo ./hosts.ini
# Run Exp
for scheme in "${schemes[@]}"; do
    echo "Start experiment of ${scheme}"
    # Load data for evaluation
    loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}"

    # Run experiment
    for currentSimulatedClientNumber in "${simulatedClientNumberSet[@]}"; do
        currentOperationNumber=$((operationNumber * simulatedClientNumber))
        for workload in "${workloads[@]}"; do
            for runningMode in "${runningTypes[@]}"; do
                # Run experiment
                doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${currentOperationNumber}" "${currentSimulatedClientNumber}" "${RunningRoundNumber}" "${runningMode}" "${workload}" "ONE"
            done
        done
    done
done
