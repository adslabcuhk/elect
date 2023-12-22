#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

# Fetch performance data from log files.
search_dir="./results"
expName=$1
scheme=$2
# runningMode=$3
# workload=$4
# recordcount=$5
# operationcount=$6
# key_length=$7
# field_length=$8
# threads=$9
possibleOperationTypeSet=(READ INSERT SCAN UPDATE)
benchmarkTypeSet=("AverageLatency" "99thPercentileLatency")
# benchmarkTypeSet=("AverageLatency" "25thPercentileLatency" "50thPercentileLatency" "75thPercentileLatency" "90thPercentileLatency" "99thPercentileLatency")

function calculate {
    values=("$@")
    num_elements=${#values[@]}

    if [ $num_elements -eq 1 ]; then
        echo "Only one round: ${values[0]}"
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
        echo "Average: $avg, Min: $min, Max: $max"
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

function processLoadLogs {
    filePathList=("$@")
    declare -A results_dict
    throughput_values=()
    for file in "${filePathList[@]}"; do
        # echo "Processing file ${file}"
        for param in "${benchmarkTypeSet[@]}"; do
            while IFS= read -r line; do
                results_dict[$param]+="${line} "
            done < <(grep "\[INSERT\], ${param}" "${file}" | awk -F',' '{print $NF}')
        done
        while IFS= read -r line; do
            throughput_values+=("$line")
        done < <(cat "${file}" | grep "Throughput" | awk -F',' '{print $NF}')
    done
    echo -e "\033[31;1mThroughput (unit: op/s): \033[0m"
    calculate "${throughput_values[@]}"
    for param in "${benchmarkTypeSet[@]}"; do
        if [ "${param} " == "AverageLatency " ]; then
            echo -e "\033[31;1mAverage operation latency (unit: us):\033[0m"
        elif [ "${param} " == "25thPercentileLatency " ]; then
            echo -e "\033[31;1m25th percentile latency (unit: us):\033[0m"
        elif [ "${param} " == "50thPercentileLatency " ]; then
            echo -e "\033[31;1m50th percentile latency (unit: us):\033[0m"
        elif [ "${param} " == "75thPercentileLatency " ]; then
            echo -e "\033[31;1m75th percentile latency (unit: us):\033[0m"
        elif [ "${param} " == "90thPercentileLatency " ]; then
            echo -e "\033[31;1m90th percentile latency (unit: us):\033[0m"
        elif [ "${param} " == "99thPercentileLatency " ]; then
            echo -e "\033[31;1m99th percentile latency (unit: us):\033[0m"
        fi

        IFS=' ' read -r -a normalArray <<<"${results_dict[$param]}"
        calculate "${normalArray[@]}"
    done
}

function processRunLogs {
    filePathList=("$@")
    declare -A results_dict
    throughput_values=()

    for file in "${filePathList[@]}"; do
        for operationType in "${possibleOperationTypeSet[@]}"; do
            for param in "${benchmarkTypeSet[@]}"; do
                while IFS= read -r line; do
                    key="${operationType}_${param}"
                    results_dict[$key]+="${line} "
                done < <(grep "\[${operationType}\], ${param}" "${file}" | awk -F',' '{print $NF}')
            done
        done

        while IFS= read -r line; do
            throughput_values+=("$line")
        done < <(grep "Throughput" "${file}" | awk -F',' '{print $NF}')
    done

    echo -e "\033[31;1mThroughput (unit: op/s): \033[0m"
    calculate "${throughput_values[@]}"

    for operationType in "${possibleOperationTypeSet[@]}"; do
        for param in "${benchmarkTypeSet[@]}"; do
            key="${operationType}_${param}"
            if [[ ! -z "${results_dict[$key]}" ]]; then
                if [ "${param} " == "AverageLatency " ]; then
                    echo -e "\033[31;1m[${operationType}] Average operation latency (unit: us):\033[0m"
                elif [ "${param} " == "25thPercentileLatency " ]; then
                    echo -e "\033[31;1m[${operationType}] 25th percentile latency (unit: us):\033[0m"
                elif [ "${param} " == "50thPercentileLatency " ]; then
                    echo -e "\033[31;1m[${operationType}] 50th percentile latency (unit: us):\033[0m"
                elif [ "${param} " == "75thPercentileLatency " ]; then
                    echo -e "\033[31;1m[${operationType}] 75th percentile latency (unit: us):\033[0m"
                elif [ "${param} " == "90thPercentileLatency " ]; then
                    echo -e "\033[31;1m[${operationType}] 90th percentile latency (unit: us):\033[0m"
                elif [ "${param} " == "99thPercentileLatency " ]; then
                    echo -e "\033[31;1m[${operationType}] 99th percentile latency (unit: us):\033[0m"
                fi
                IFS=' ' read -r -a normalArray <<<"${results_dict[$key]}"
                calculate "${normalArray[@]}"
            fi
        done
    done
}

function processLoadResults {
    declare -A file_dict
    for file in "$search_dir"/*; do
        if [[ $file =~ ${expName}-Load-Scheme-${scheme}-workload([^/]+)-KVNumber-([0-9]+)-KeySize-([0-9]+)-ValueSize-([0-9]+)-ClientNumber-([0-9]+)-Time-[0-9]+ ]]; then
            key="scheme: ${scheme}, workload: ${BASH_REMATCH[1]}, KVNumber: ${BASH_REMATCH[2]}, KeySize: ${BASH_REMATCH[3]}, ValueSize: ${BASH_REMATCH[4]}, ClientNumber: ${BASH_REMATCH[5]}"
            file_dict[$key]+="$file "
            # echo "add file $file to category $key"
        fi
    done

    for key in "${!file_dict[@]}"; do
        echo "[Exp info] $key"
        IFS=' ' read -r -a normalArray <<<"${file_dict[$key]}"
        processLoadLogs "${normalArray[@]}"
    done
}

function processNormalResults {
    declare -A file_dict
    for file in "$search_dir"/*; do
        if [[ $file =~ ${expName}-Run-normal-Scheme-${scheme}-workload([^/]+)-KVNumber-([0-9]+)-KeySize-([0-9]+)-ValueSize-([0-9]+)-ClientNumber-([0-9]+)-Time-[0-9]+ ]]; then
            key="scheme: ${scheme}, workload: ${BASH_REMATCH[1]}, KVNumber: ${BASH_REMATCH[2]}, KeySize: ${BASH_REMATCH[3]}, ValueSize: ${BASH_REMATCH[4]}, ClientNumber: ${BASH_REMATCH[5]}"
            file_dict[$key]+="$file "
            # echo "add file $file to category $key"
        fi
    done

    for key in "${!file_dict[@]}"; do
        echo "[Exp info] $key"
        IFS=' ' read -r -a normalArray <<<"${file_dict[$key]}"
        processRunLogs "${normalArray[@]}"
    done
}

function processDegradedResults {
    declare -A file_dict
    for file in "$search_dir"/*; do
        if [[ $file =~ ${expName}-Run-normal-Scheme-${scheme}-workload([^/]+)-KVNumber-([0-9]+)-KeySize-([0-9]+)-ValueSize-([0-9]+)-ClientNumber-([0-9]+)-Time-[0-9]+ ]]; then
            key="scheme: ${scheme}, workload: ${BASH_REMATCH[1]}, KVNumber: ${BASH_REMATCH[2]}, KeySize: ${BASH_REMATCH[3]}, ValueSize: ${BASH_REMATCH[4]}, ClientNumber: ${BASH_REMATCH[5]}"
            file_dict[$key]+="$file "
            # echo "add file $file to category $key"
        fi
    done

    for key in "${!file_dict[@]}"; do
        echo "[Exp info] $key"
        IFS=' ' read -r -a normalArray <<<"${file_dict[$key]}"
        processRunLogs "${normalArray[@]}"
    done
}

processNormalResults
