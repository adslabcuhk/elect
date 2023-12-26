#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

expName=$1
targetScheme=$2
KVNumber=$3
keylength=$4
fieldlength=$5
OPNumber=$6
ClientNumber=$7
outputType=$8
ConsistencyLevel=${9:-"ONE"}
shift 9
codingK=${1:-4}
storageSavingTarget=${2:-0.6}

function calculate {
    values=("$@")
    num_elements=${#values[@]}

    if [ $num_elements -eq 1 ]; then
        printf "Only one round: %.2f\n" "${values[0]}"
    elif [ $num_elements -lt 5 ]; then
        min=${values[0]}
        max=${values[0]}
        sum=0
        for i in "${values[@]}"; do
            if (($(echo "$i < $min" | bc -l))); then
                min=$i
            fi
            if (($(echo "$i > $max" | bc -l))); then
                max=$i
            fi
            sum=$(echo "$sum + $i" | bc -l)
        done
        avg=$(echo "scale=2; $sum / $num_elements" | bc -l)
        min_formatted=$(printf "%.2f" $min)
        max_formatted=$(printf "%.2f" $max)
        echo "Average: $avg, Min: $min_formatted, Max: $max_formatted"
    elif [ $num_elements -ge 5 ]; then
        values_csv=$(printf ",%.2f" "${values[@]}")
        values_csv=${values_csv:1}
        python3 -c "
import numpy as np
import scipy.stats

data = np.array([${values_csv[*]}])
confidence = 0.95
mean = np.mean(data)
sem = scipy.stats.sem(data)
interval = scipy.stats.t.interval(confidence, len(data)-1, loc=mean, scale=sem)

print('Average: {:.2f}; The 95% confidence interval: ({:.2f}, {:.2f})'.format(mean, interval[0], interval[1]))
"
    fi
}

memtableTimeCostList=()
commitLogTimeCostList=()
flushTimeCostList=()
compactionTimeCostList=()
rewriteTimeCostList=()
ecsstableCompactionTimeCostList=()
encodingTimeCostList=()
migrateRawSSTableTimeCostList=()
migrateRawSSTableSendTimeCostList=()
migrateParityCodeTimeCostList=()
readIndexTimeCostList=()
readCacheTimeCostList=()
readMemtableTimeCostList=()
readSSTableTimeCostList=()
readMigratedRawDataTimeCostList=()
waitForMigrationTimeCostList=()
waitForRecoveryTimeCostList=()
retrieveTimeCostList=()
decodingTimeCostList=()
readMigratedParityTimeCostList=()
retrieveLsmTreeTimeCostList=()
recoveryLsmTreeTimeCostList=()

WALSumList=()
memtableSumList=()
flushSumList=()
compactionSumList=()
transitioningSumList=()
migrationSumList=()
recoverySumList=()
readCacheSumList=()
readMemtableSumList=()
readSSTableSumList=()

function clearBuffer {
    memtableTimeCostList=()
    commitLogTimeCostList=()
    flushTimeCostList=()
    compactionTimeCostList=()
    rewriteTimeCostList=()
    ecsstableCompactionTimeCostList=()
    encodingTimeCostList=()
    migrateRawSSTableTimeCostList=()
    migrateRawSSTableSendTimeCostList=()
    migrateParityCodeTimeCostList=()
    readIndexTimeCostList=()
    readCacheTimeCostList=()
    readMemtableTimeCostList=()
    readSSTableTimeCostList=()
    readMigratedRawDataTimeCostList=()
    waitForMigrationTimeCostList=()
    waitForRecoveryTimeCostList=()
    retrieveTimeCostList=()
    decodingTimeCostList=()
    readMigratedParityTimeCostList=()
    retrieveLsmTreeTimeCostList=()
    recoveryLsmTreeTimeCostList=()
}

function fetchContent {
    filedir=$1
    opType=$2
    file=""
    if [ "${opType}" == "Load" ]; then
        file=${filedir}/${expName}-${targetScheme}-Load_workloadLoad_After-flush-compaction_db_stats.txt
    elif [ "${opType}" == "read" ]; then
        file=${filedir}/${expName}_workloadRead_After-run_db_stats.txt
    fi
    fileContent=$(cat "${file}")

    memtableTimeCost=$(echo "$fileContent" | grep -oP 'Memtable time cost: \K[0-9]+')
    commitLogTimeCost=$(echo "$fileContent" | grep -oP 'CommitLog time cost: \K[0-9]+')
    flushTimeCost=$(echo "$fileContent" | grep -oP 'Flush time cost: \K[0-9]+')
    compactionTimeCost=$(echo "$fileContent" | grep -oP 'Compaction time cost: \K[0-9]+')
    rewriteTimeCost=$(echo "$fileContent" | grep -oP 'Rewrite time cost: \K[0-9]+')
    ecsstableCompactionTimeCost=$(echo "$fileContent" | grep -oP 'ECSSTable compaction time cost: \K[0-9]+')
    encodingTimeCost=$(echo "$fileContent" | grep -oP 'Encoding time cost: \K[0-9]+')
    migrateRawSSTableTimeCost=$(echo "$fileContent" | grep -oP 'Migrate raw SSTable time cost: \K[0-9]+')
    migrateRawSSTableSendTimeCost=$(echo "$fileContent" | grep -oP 'Migrate raw SSTable send time cost: \K[0-9]+')
    migrateParityCodeTimeCost=$(echo "$fileContent" | grep -oP 'Migrate parity code time cost: \K[0-9]+')

    # "Read operations"
    readIndexTimeCost=$(echo "$fileContent" | grep -oP 'Read index time cost: \K[0-9]+')
    readCacheTimeCost=$(echo "$fileContent" | grep -oP 'Read cache time cost: \K[0-9]+')
    readMemtableTimeCost=$(echo "$fileContent" | grep -oP 'Read memtable time cost: \K[0-9]+')
    readSSTableTimeCost=$(echo "$fileContent" | grep -oP 'Read SSTable time cost: \K[0-9]+')
    readMigratedRawDataTimeCost=$(echo "$fileContent" | grep -oP 'Read migrated raw data time cost: \K[0-9]+')
    waitForMigrationTimeCost=$(echo "$fileContent" | grep -oP 'Wait for migration time cost: \K[0-9]+')
    waitForRecoveryTimeCost=$(echo "$fileContent" | grep -oP 'Wait for recovery time cost: \K[0-9]+')
    retrieveTimeCost=$(echo "$fileContent" | grep -oP 'Retrieve time cost: \K[0-9]+')
    decodingTimeCost=$(echo "$fileContent" | grep -oP 'Decoding time cost: \K[0-9]+')
    readMigratedParityTimeCost=$(echo "$fileContent" | grep -oP 'Read migrated parity time cost: \K[0-9]+')

    # "Recovery operations"
    retrieveLsmTreeTimeCost=$(echo "$fileContent" | grep -oP 'Retrieve LSM tree time cost: \K[0-9]+')
    recoveryLsmTreeTimeCost=$(echo "$fileContent" | grep -oP 'Recovery LSM tree time cost: \K[0-9]+')

    memtableTimeCostList+=(${memtableTimeCost})
    commitLogTimeCostList+=(${commitLogTimeCost})
    flushTimeCostList+=(${flushTimeCost})
    compactionTimeCostList+=(${compactionTimeCost})
    rewriteTimeCostList+=(${rewriteTimeCost})
    ecsstableCompactionTimeCostList+=(${ecsstableCompactionTimeCost})
    encodingTimeCostList+=(${encodingTimeCost})
    migrateRawSSTableTimeCostList+=(${migrateRawSSTableTimeCost})
    migrateRawSSTableSendTimeCostList+=(${migrateRawSSTableSendTimeCost})
    migrateParityCodeTimeCostList+=(${migrateParityCodeTimeCost})
    readIndexTimeCostList+=(${readIndexTimeCost})
    readCacheTimeCostList+=(${readCacheTimeCost})
    readMemtableTimeCostList+=(${readMemtableTimeCost})
    readSSTableTimeCostList+=(${readSSTableTimeCost})
    readMigratedRawDataTimeCostList+=(${readMigratedRawDataTimeCost})
    waitForMigrationTimeCostList+=(${waitForMigrationTimeCost})
    waitForRecoveryTimeCostList+=(${waitForRecoveryTimeCost})
    retrieveTimeCostList+=(${retrieveTimeCost})
    decodingTimeCostList+=(${decodingTimeCost})
    readMigratedParityTimeCostList+=(${readMigratedParityTimeCost})
    retrieveLsmTreeTimeCostList+=(${retrieveLsmTreeTimeCost})
    recoveryLsmTreeTimeCostList+=(${recoveryLsmTreeTimeCost})
}

function calculateWithOperationDataSizeForMS {
    totalDataSizeInMiB=$(echo "scale=2; ${OPNumber} * (${keylength} + ${fieldlength}) / 1024 / 1024" | bc -l)
    targetArray=("$@")
    newArray=()
    arrayLength=${#targetArray[@]}
    for ((i = 0; i < $arrayLength; i++)); do
        sum=$(echo "scale=2; ${targetArray[i]} / $totalDataSizeInMiB" | bc -l)
        newArray+=($sum)
    done
    calculate "${newArray[@]}"
}

function calculateWithOperationDataSizeForNS {
    totalDataSizeInMiB=$(echo "scale=2; ${OPNumber} * (${keylength} + ${fieldlength}) / 1024 / 1024" | bc -l)
    targetArray=("$@")
    newArray=()
    arrayLength=${#targetArray[@]}
    for ((i = 0; i < $arrayLength; i++)); do
        sum=$(echo "scale=2; ${targetArray[i]} / 1000000 / $totalDataSizeInMiB" | bc -l)
        newArray+=($sum)
    done
    calculate "${newArray[@]}"
}

function calculateWithOperationDataSizeForUS {
    totalDataSizeInMiB=$(echo "scale=2; ${OPNumber} * (${keylength} + ${fieldlength}) / 1024 / 1024" | bc -l)
    targetArray=("$@")
    newArray=()
    arrayLength=${#targetArray[@]}
    for ((i = 0; i < $arrayLength; i++)); do
        sum=$(echo "scale=2; ${targetArray[i]} / 1000 / $totalDataSizeInMiB" | bc -l)
        newArray+=($sum)
    done
    calculate "${newArray[@]}"
}

function sumArray {
    local targetArray=("$@")
    local sum=0
    for item in "${targetArray[@]}"; do
        sum=$((sum + item))
    done
    echo $sum
}

function processBreakdownResultsForLoad {
    declare -A files_by_node=()
    for file in "${PathToELECTResultSummary}/${targetScheme}"/*; do
        if [[ -d $file ]]; then
            if [[ $file =~ ${expName}-Load-KVNumber-${KVNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-CodingK-${codingK}-Saving-${storageSavingTarget}-Node-([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)-Time-[0-9]+ ]]; then
                nodeID=${BASH_REMATCH[1]}
                files_by_node[$nodeID]+="${file} "
            fi
        fi
    done
    runningRoundNumber=0
    for nodeID in "${!files_by_node[@]}"; do
        # echo "NodeID: $nodeID, Files: ${files_by_node[$nodeID]}"
        IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
        runningRoundNumber=${#files_array[@]}
    done
    for i in $(seq 0 $((runningRoundNumber - 1))); do
        declare -a ith_files=()
        for nodeID in "${!files_by_node[@]}"; do
            IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
            if [ $i -lt ${#files_array[@]} ]; then
                ith_files+=("${files_array[i]}")
            else
                ith_files+=("")
            fi
        done
        # echo "${i}-th round: ${ith_files[*]}"
        for file in "${ith_files[@]}"; do
            fetchContent "${file}" "Load"
        done
        WALSumList+=("$(sumArray "${commitLogTimeCostList[@]}")")
        memtableSumList+=("$(sumArray "${memtableTimeCostList[@]}")")
        flushSumList+=("$(sumArray "${flushTimeCostList[@]}")")
        compactionSumList+=("$(sumArray "${compactionTimeCostList[@]}")")
        transitioningArray=()
        arrayLengthTransition=${#rewriteTimeCostList[@]}
        for ((i = 0; i < $arrayLengthTransition; i++)); do
            sum=$(echo "scale=2; ${rewriteTimeCostList[i]} + ${ecsstableCompactionTimeCostList[i]} + ${encodingTimeCostList[i]}" | bc -l)
            transitioningArray+=($sum)
        done
        transitioningSumList+=("$(sumArray "${transitioningArray[@]}")")
        migrationArray=()
        arrayLengthMigration=${#migrateRawSSTableTimeCostList[@]}
        for ((i = 0; i < $arrayLengthMigration; i++)); do
            sum=$(echo "scale=2; ${migrateRawSSTableTimeCostList[i]} + ${migrateParityCodeTimeCostList[i]}" | bc -l)
            migrationArray+=($sum)
        done
        migrationSumList+=("$(sumArray "${migrationArray[@]}")")
        clearBuffer
    done
    # output
    echo -e "\033[1m\033[34m[Breakdown info for Write] scheme: ${targetScheme}, KVNumber: ${KVNumber}, KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"
    echo -e "\033[31;1mWAL (unit: ms/MiB):\033[0m"
    calculateWithOperationDataSizeForMS "${WALSumList[@]}"
    echo -e "\033[31;1mMemTable (unit: ms/MiB):\033[0m"
    calculateWithOperationDataSizeForMS "${memtableSumList[@]}"
    echo -e "\033[31;1mFlushing (unit: ms/MiB):\033[0m"
    calculateWithOperationDataSizeForMS "${flushSumList[@]}"
    echo -e "\033[31;1mCompaction (unit: ms/MiB):\033[0m"
    calculateWithOperationDataSizeForMS "${compactionSumList[@]}"
    if [ "${targetScheme}" == "elect" ]; then
        echo -e "\033[31;1mTransitioning (unit: ms/MiB):\033[0m"
        calculateWithOperationDataSizeForMS "${transitioningSumList[@]}"
        echo -e "\033[31;1mMigration (unit: ms/MiB):\033[0m"
        calculateWithOperationDataSizeForMS "${migrationSumList[@]}"
    fi
}

function processBreakdownResultsForNormalRead {
    declare -A files_by_node=()
    for file in "${PathToELECTResultSummary}/${targetScheme}"/*; do
        if [[ -d $file ]]; then
            if [[ $file =~ ${expName}-Run-normal-Workload-workloadRead-KVNumber-${KVNumber}-OPNumber-${OPNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-ClientNumber-${ClientNumber}-Consistency-${ConsistencyLevel}-Node-([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)-Time-[0-9]+ ]]; then
                nodeID=${BASH_REMATCH[1]}
                files_by_node[$nodeID]+="${file} "
            fi
        fi
    done
    runningRoundNumber=0
    for nodeID in "${!files_by_node[@]}"; do
        # echo "NodeID: $nodeID, Files: ${files_by_node[$nodeID]}"
        IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
        runningRoundNumber=${#files_array[@]}
    done

    for i in $(seq 0 $((runningRoundNumber - 1))); do
        declare -a ith_files=()
        for nodeID in "${!files_by_node[@]}"; do
            IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
            if [ $i -lt ${#files_array[@]} ]; then
                ith_files+=("${files_array[i]}")
            else
                ith_files+=("")
            fi
        done
        # echo "${i}-th round: ${ith_files[*]}"
        for file in "${ith_files[@]}"; do
            fetchContent "${file}" "read"
        done
        readCacheSumList+=("$(sumArray "${readCacheTimeCostList[@]}")")
        readMemtableSumList+=("$(sumArray "${readMemtableTimeCostList[@]}")")
        readSSTableSumList+=("$(sumArray "${readSSTableTimeCostList[@]}")")
        clearBuffer
    done
    # output
    echo -e "\033[1m\033[34m[Breakdown info for normal Read] scheme: ${targetScheme}, KVNumber: ${KVNumber}, OPNumber: ${OPNumber} KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"
    echo -e "\033[31;1mCache (unit: ms/MiB):\033[0m"
    calculateWithOperationDataSizeForNS "${readCacheSumList[@]}"
    echo -e "\033[31;1mMemTable (unit: ms/MiB):\033[0m"
    calculateWithOperationDataSizeForMS "${readMemtableSumList[@]}"
    echo -e "\033[31;1mSSTables (unit: ms/MiB):\033[0m"
    calculateWithOperationDataSizeForMS "${readSSTableSumList[@]}"
}

function processBreakdownResultsForDegradedRead {
    declare -A files_by_node=()
    for file in "${PathToELECTResultSummary}/${targetScheme}"/*; do
        if [[ -d $file ]]; then
            if [[ $file =~ ${expName}-Run-degraded-Workload-workloadRead-KVNumber-${KVNumber}-OPNumber-${OPNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-ClientNumber-${ClientNumber}-Consistency-${ConsistencyLevel}-Node-([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)-Time-[0-9]+ ]]; then
                nodeID=${BASH_REMATCH[1]}
                files_by_node[$nodeID]+="${file} "
            fi
        fi
    done
    runningRoundNumber=0
    for nodeID in "${!files_by_node[@]}"; do
        # echo "NodeID: $nodeID, Files: ${files_by_node[$nodeID]}"
        IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
        runningRoundNumber=${#files_array[@]}
    done

    for i in $(seq 0 $((runningRoundNumber - 1))); do
        declare -a ith_files=()
        for nodeID in "${!files_by_node[@]}"; do
            IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
            if [ $i -lt ${#files_array[@]} ]; then
                ith_files+=("${files_array[i]}")
            else
                ith_files+=("")
            fi
        done
        # echo "${i}-th round: ${ith_files[*]}"
        for file in "${ith_files[@]}"; do
            fetchContent "${file}" "read"
        done
        readCacheSumList+=("$(sumArray "${readCacheTimeCostList[@]}")")
        readMemtableSumList+=("$(sumArray "${readMemtableTimeCostList[@]}")")
        newReadSSTArray=()
        arrayLength=${#readSSTableTimeCostList[@]}
        for ((index = 0; index < $arrayLength; index++)); do
            sum=$(echo "scale=2; ${readSSTableTimeCostList[index]} * 1000 - ${waitForRecoveryTimeCostList[index]}" | bc -l)
            newReadSSTArray+=($sum)
        done
        readSSTableSumList+=("$(sumArray "${newReadSSTArray[@]}")")
        recoverySumList+=("$(sumArray "${waitForRecoveryTimeCostList[@]}")")
        clearBuffer
    done
    # output
    echo -e "\033[1m\033[34m[Breakdown info for degraded Read] scheme: ${targetScheme}, KVNumber: ${KVNumber}, OPNumber: ${OPNumber} KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"
    echo -e "\033[31;1mCache (unit: ms/MiB):\033[0m"
    calculateWithOperationDataSizeForNS "${readCacheSumList[@]}"
    echo -e "\033[31;1mMemTable (unit: ms/MiB):\033[0m"
    calculateWithOperationDataSizeForMS "${readMemtableSumList[@]}"
    echo -e "\033[31;1mSSTables (unit: ms/MiB):\033[0m"
    calculateWithOperationDataSizeForUS "${readSSTableSumList[@]}"
    if [ "${targetScheme}" == "elect" ]; then
        echo -e "\033[31;1mRecovery (unit: ms/MiB):\033[0m"
        calculateWithOperationDataSizeForUS "${recoverySumList[@]}"
    fi
}

if [ "${outputType}" == "Load" ]; then
    processBreakdownResultsForLoad
    echo ""
elif [ "${outputType}" == "normal" ]; then
    processBreakdownResultsForNormalRead
    echo ""
elif [ "${outputType}" == "degraded" ]; then
    processBreakdownResultsForDegradedRead
    echo ""
fi
