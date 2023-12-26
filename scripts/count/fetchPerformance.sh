#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

# Fetch performance data from log files.
search_dir=$PathToELECTResultSummary
fetchType=$1
expName=$2
scheme=$3
# runningMode=$3
# workload=$4
# recordcount=$5
# operationcount=$6
# key_length=$7
# field_length=$8
# threads=$9
possibleOperationTypeSet=(READ INSERT SCAN UPDATE)
benchmarkTypeSet=("AverageLatency" "99thPercentileLatency")

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
        if [[ $file =~ ${expName}-Load-Scheme-${scheme}-workload([^/]+)-KVNumber-([0-9]+)-KeySize-([0-9]+)-ValueSize-([0-9]+)-CodingK-([0-9]+)-Saving-([0-9]+\.[0-9]+)-ClientNumber-([0-9]+)-Time-[0-9]+ ]]; then
            key="scheme: ${scheme}, workload: ${BASH_REMATCH[1]}, KVNumber: ${BASH_REMATCH[2]}, KeySize: ${BASH_REMATCH[3]}, ValueSize: ${BASH_REMATCH[4]}, Coding K: ${BASH_REMATCH[5]}, Storage saving target: ${BASH_REMATCH[6]}, ClientNumber: ${BASH_REMATCH[7]}"
            file_dict[$key]+="$file "
            # echo "add file $file to category $key"
        fi
    done

    for key in "${!file_dict[@]}"; do
        echo -e "\033[1m\033[34m[Exp info] $key\033[0m"
        IFS=' ' read -r -a normalArray <<<"${file_dict[$key]}"
        processLoadLogs "${normalArray[@]}"
    done
}

function processNormalResults {
    declare -A file_dict
    for file in "$search_dir"/*; do
        if [[ $file =~ ${expName}-Run-normal-Scheme-${scheme}-workload([^/]+)-KVNumber-([0-9]+)-OPNumber-([0-9]+)-KeySize-([0-9]+)-ValueSize-([0-9]+)-ClientNumber-([0-9]+)-Consistency-(ONE|TWO|ALL)((-[^-]+)*)-Time-[0-9]+ ]]; then
            key="scheme: ${scheme}, workload: ${BASH_REMATCH[1]}, KVNumber: ${BASH_REMATCH[2]}, OPNumber: ${BASH_REMATCH[3]}, KeySize: ${BASH_REMATCH[4]}, ValueSize: ${BASH_REMATCH[5]}, ClientNumber: ${BASH_REMATCH[6]}, ConsistencyLevel: ${BASH_REMATCH[7]}, ExtraFlag: ${BASH_REMATCH[8]}"
            file_dict[$key]+="$file "
            # echo "add file $file to category $key"
        fi
    done

    for key in "${!file_dict[@]}"; do
        echo -e "\033[1m\033[34m[Exp info] $key\033[0m"
        IFS=' ' read -r -a normalArray <<<"${file_dict[$key]}"
        processRunLogs "${normalArray[@]}"
    done
}

# test-Run-degraded-Scheme-elect-workloadScan-KVNumber-600000-OPNumber-60000-KeySize-24-ValueSize-1000-ClientNumber-16-Consistency-ONE-Time-1703254370.log
function processDegradedResults {
    declare -A file_dict
    for file in "$search_dir"/*; do
        if [[ $file =~ ${expName}-Run-degraded-Scheme-${scheme}-workload([^/]+)-KVNumber-([0-9]+)-OPNumber-([0-9]+)-KeySize-([0-9]+)-ValueSize-([0-9]+)-ClientNumber-([0-9]+)-Consistency-(ONE|TWO|ALL)((-[^-]+)*)-Time-[0-9]+ ]]; then
            key="scheme: ${scheme}, workload: ${BASH_REMATCH[1]}, KVNumber: ${BASH_REMATCH[2]}, OPNumber: ${BASH_REMATCH[3]}, KeySize: ${BASH_REMATCH[4]}, ValueSize: ${BASH_REMATCH[5]}, ClientNumber: ${BASH_REMATCH[6]}, ConsistencyLevel: ${BASH_REMATCH[7]}, ExtraFlag: ${BASH_REMATCH[8]}"
            file_dict[$key]+="$file "
            # echo "add file $file to category $key"
        fi
    done

    for key in "${!file_dict[@]}"; do
        echo -e "\033[1m\033[34m[Exp info] $key\033[0m"
        IFS=' ' read -r -a normalArray <<<"${file_dict[$key]}"
        processRunLogs "${normalArray[@]}"
    done
}

if [ "${fetchType} " == "load " ]; then
    echo -e "\033[1mResults of Loading data:\033[0m"
    processLoadResults
    echo ""
elif [ "${fetchType} " == "normal " ]; then
    echo -e "\033[1mResults of benchmark without node failures (normal operations):\033[0m"
    processNormalResults
    echo ""
elif [ "${fetchType} " == "degraded " ]; then
    echo -e "\033[1mResults of benchmark with node failures (degraded operations):\033[0m"
    processDegradedResults
    echo ""
elif [ "${fetchType} " == "all " ]; then
    echo -e "\033[1mResults of Loading data:\033[0m"
    processLoadResults
    echo ""
    echo -e "\033[1mResults of benchmark without node failures (normal operations):\033[0m"
    processNormalResults
    echo ""
    echo -e "\033[1mResults of benchmark with node failures (degraded operations):\033[0m"
    processDegradedResults
    echo ""
fi
