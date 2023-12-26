#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

expName=$1
targetScheme=$2
KVNumber=$3
keylength=$4
fieldlength=$5

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

totalRecoveryTimeCostListForELECT=()
totalRetrieveCostListForELECT=()
totalDecodeTimeCostListForELECT=()
totalRepairCostListForCassandra=()

function generateForELECT {
    filePathList=("$@")
    for file in "${filePathList[@]}"; do
        totalRetrieveCost=0
        totalDecodeTimeCost=0
        while IFS= read -r line; do
            if [[ $line == Retrieve\ File\ Cost:* ]]; then
                cost=$(echo $line | grep -o -E '[0-9]+')
                totalRetrieveCost=$((totalRetrieveCost + cost))
            fi
            if [[ $line == Decode\ Time\ Cost:* ]]; then
                cost=$(echo $line | grep -o -E '[0-9]+')
                totalDecodeTimeCost=$((totalDecodeTimeCost + cost))
            fi
        done <"${file}"
        totalRetrieveCostListForELECT+=("$totalRetrieveCost")
        totalDecodeTimeCostListForELECT+=("$totalDecodeTimeCost")
    done
    # Count the total recovery time cost
    arrayLength=${#totalRetrieveCostListForELECT[@]}
    for ((i = 0; i < $arrayLength; i++)); do
        sum=$((totalRetrieveCostListForELECT[i] + totalDecodeTimeCostListForELECT[i]))
        totalRecoveryTimeCostListForELECT+=($sum)
    done
}

function generateForCassandra {
    filePathList=("$@")
    for file in "${filePathList[@]}"; do
        repairTimeCost=0
        while IFS= read -r line; do
            if [[ $line == Repair\ Time\ Cost:* ]]; then
                repairTimeCost=$(echo $line | awk '{print $4}')
                break
            fi
        done <"${file}"
        totalRepairCostListForCassandra+=("$repairTimeCost")
    done
}

function processRecoveryResults {
    file_dict=()
    for file in "$PathToELECTResultSummary"/*; do
        if [[ $file =~ ${expName}-Scheme-${targetScheme}-Size-${KVNumber}-recovery-Round-[0-9]+-RecoverNode-[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+-Time-[0-9]+ ]]; then
            file_dict+=("$file")
        fi
    done
    # echo "${file_dict[@]}"
    if [ "$targetScheme" == "elect" ]; then
        generateForELECT "${file_dict[@]}"
        echo -e "\033[1m\033[34m[Exp info] scheme: ${targetScheme}, KVNumber: ${KVNumber}, KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"
        echo -e "\033[31;1mTotal recovery time cost (unit: s):\033[0m"
        calculate "${totalRecoveryTimeCostListForELECT[@]}"
        echo -e "\033[31;1mRecovery time cost for retrieving LSM-trees (unit: s):\033[0m"
        calculate "${totalRetrieveCostListForELECT[@]}"
        echo -e "\033[31;1mRecovery time cost for decode SSTables (unit: s):\033[0m"
        calculate "${totalDecodeTimeCostListForELECT[@]}"
    else
        generateForCassandra "${file_dict[@]}"
        echo -e "\033[1m\033[34m[Exp info] scheme: ${targetScheme}, KVNumber: ${KVNumber}, KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"
        echo -e "\033[31;1mTotal recovery time cost (unit: s):\033[0m"
        calculate "${totalRepairCostListForCassandra[@]}"
    fi
}

processRecoveryResults
echo ""
