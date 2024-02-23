#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
# Exp-Simple: YCSB core workloads, 3-way replication, (6,4) encoding, 60% target storage saving, 0.3M KV load.

ExpName="Exp-simple"
schemes=("elect")
KVNumber=240000
keyLength=24
valueLength=1000
simulatedClientNumber=${defaultSimulatedClientNumber}
dataSizeEstimation=$(echo "scale=2; $KVNumber * ($keyLength + $valueLength) / 1024 / 1024 / 1024 * 3 * 1.75" | bc)

# Setup hosts
setupNodeInfo ./hosts.ini
# Run Exp
for scheme in "${schemes[@]}"; do
    echo "Start experiment of ${scheme}"
    # Load data for evaluation
    loadDataForBoFFastTest "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${simulatedClientNumber}"
done

# Generate the summarized results
if [ -f "${PathToScripts}/exp/${ExpName}.log" ]; then
    rm -rf "${PathToScripts}/exp/${ExpName}.log"
fi
# output storage usage
echo "The storage overhead results:" >>${PathToScripts}/exp/${ExpName}.log
echo "The estimated storage overhead (with 3-way replication, include metadata and Cassandra Logs): ${dataSizeEstimation} GiB" >>${PathToScripts}/exp/${ExpName}.log
for scheme in "${schemes[@]}"; do
    echo "Storage usage of ${scheme}" >>${PathToScripts}/exp/${ExpName}.log
    ${PathToScripts}/count/fetchStorage.sh "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" >>${PathToScripts}/exp/${ExpName}.log
done
cat ${PathToScripts}/exp/${ExpName}.log
