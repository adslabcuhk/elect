#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
PathToELECTResultSummary=${PathToScripts}/count/results

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

calculate_combined_cpu_95th_percentile() {
    local -a files=("$@")
    local -a combined_cpu_loads
    local min_lines length i j cpu cpu_cores

    cores_per_node=$(nproc)
    # cores_per_node=160

    declare -a valid_files
    min_lines=$(wc -l <"${files[0]}")
    for file in "${files[@]}"; do
        local lines=$(wc -l <"$file")
        if [ $lines -lt 5 ]; then
            continue
        fi
        ((lines < min_lines)) && min_lines=$lines
        valid_files+=("$file")
    done
    cpu_cores=$((cores_per_node * ${#valid_files[@]}))
    for ((i = 0; i < min_lines; i++)); do
        local sum_line=0
        for file in "${valid_files[@]}"; do
            cpu=$(sed -n "$((i + 1))p" "$file" | grep -oP 'CPU Load: \K[0-9.]+')
            [[ -n $cpu ]] && sum_line=$(echo "$sum_line + $cpu" | bc)
        done
        combined_cpu_loads+=("$sum_line")
    done

    IFS=$'\n' sorted_combined_cpu_loads=($(sort -n <<<"${combined_cpu_loads[*]}"))

    length=${#sorted_combined_cpu_loads[@]}
    percentile_index=$((length * 95 / 100))
    percentile_index=$((percentile_index == 0 ? 1 : percentile_index))

    combined_cpu_95th_percentile=$(echo "scale=2; ${sorted_combined_cpu_loads[$((percentile_index - 1))]} / $cpu_cores" | bc -l)

    echo "$combined_cpu_95th_percentile"
}

calculate_memory_95th_percentile() {
    local -a files=("$@")
    local -a memory_loads
    local min_lines length i j memory

    declare -a valid_files
    min_lines=$(wc -l <"${files[0]}")
    for file in "${files[@]}"; do
        local lines=$(wc -l <"$file")
        if [ $lines -lt 5 ]; then
            continue
        fi
        ((lines < min_lines)) && min_lines=$lines
        valid_files+=("$file")
    done

    for ((i = 0; i < min_lines; i++)); do
        local sum_line=0
        for file in "${files[@]}"; do
            memory=$(sed -n "$((i + 1))p" "$file" | grep -oP 'is using \K[0-9]+')
            [[ -n $memory ]] && sum_line=$(echo "$sum_line + $memory" | bc)
        done
        memory_loads+=("$sum_line")
    done

    IFS=$'\n' sorted_memory_loads=($(sort -n <<<"${memory_loads[*]}"))

    length=${#sorted_memory_loads[@]}
    percentile_index=$((length * 95 / 100))
    percentile_index=$((percentile_index == 0 ? 1 : percentile_index))

    memory_95th_percentile=${sorted_memory_loads[$((percentile_index - 1))]}

    memory_95th_percentile_in_gib=$(echo "scale=2; $memory_95th_percentile / 1048576" | bc)

    echo "$memory_95th_percentile_in_gib"
}

calculate_total_io_difference() {
    local file_before=$1
    local file_after=$2
    local read_before read_after write_before write_after
    local total_diff

    read_before=$(grep 'Total KiB read' "$file_before" | grep -oP '\d+')
    write_before=$(grep 'Total KiB written' "$file_before" | grep -oP '\d+')

    read_after=$(grep 'Total KiB read' "$file_after" | grep -oP '\d+')
    write_after=$(grep 'Total KiB written' "$file_after" | grep -oP '\d+')

    total_diff=$(((read_after - read_before) + (write_after - write_before)))
    # echo "read before $read_before, read after $read_after, write before $write_before, write after $write_after"
    echo "$total_diff"
}

calculate_network_usage_difference() {
    local file_before=$1
    local file_after=$2
    local bytes_received_before bytes_sent_before
    local bytes_received_after bytes_sent_after
    local diff_received diff_sent

    bytes_received_before=$(grep 'Bytes received' "$file_before" | grep -oP '\d+')
    bytes_sent_before=$(grep 'Bytes sent' "$file_before" | grep -oP '\d+')

    bytes_received_after=$(grep 'Bytes received' "$file_after" | grep -oP '\d+')
    bytes_sent_after=$(grep 'Bytes sent' "$file_after" | grep -oP '\d+')

    diff_received=$((bytes_received_after - bytes_received_before))
    diff_sent=$((bytes_sent_after - bytes_sent_before))

    total_diff=$((diff_received + diff_sent))
    echo "$total_diff"
}

function sumArray {
    local targetArray=("$@")
    local sum=0
    for item in "${targetArray[@]}"; do
        sum=$((sum + item))
    done
    echo $sum
}

calculate_average() {
    local -a arr=("$@")
    local sum=0
    local length=${#arr[@]}
    local average=0

    for item in "${arr[@]}"; do
        sum=$(echo "$sum + $item" | bc)
    done

    if [ $length -ne 0 ]; then
        average=$(echo "scale=2; $sum / $length" | bc)
    fi

    echo $average
}

function processResourceResultsForLoad {
    declare -A files_by_node=()
    CPUUsageSumList=()
    RAMUsageSumList=()
    DISKUsageSumList=()
    NETUsageSumList=()
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
        # CPU usage
        declare -a ith_files_CPU=()
        for nodeID in "${!files_by_node[@]}"; do
            IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
            if [ $i -lt ${#files_array[@]} ]; then
                ith_files_CPU+=("${files_array[i]}/${expName}_Loading_cpu_usage.txt")
            else
                ith_files_CPU+=("")
            fi
        done
        combined_cpu_95th_percentile=$(calculate_combined_cpu_95th_percentile "${ith_files_CPU[@]}")
        CPUUsageSumList+=($combined_cpu_95th_percentile)
        # DRAM usage
        declare -a ith_files_RAM=()
        for nodeID in "${!files_by_node[@]}"; do
            IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
            if [ $i -lt ${#files_array[@]} ]; then
                ith_files_RAM+=("${files_array[i]}/${expName}_Loading_memory_usage.txt")
            else
                ith_files_RAM+=("")
            fi
        done
        combined_ram_95th_percentile=$(calculate_memory_95th_percentile "${ith_files_RAM[@]}")
        RAMUsageSumList+=($combined_ram_95th_percentile)
        # Disk IO usage
        nodeNumberForDiskIO=${#files_by_node[@]}
        declare -a ith_files_DISK=()
        for nodeID in "${!files_by_node[@]}"; do
            IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
            if [ $i -lt ${#files_array[@]} ]; then
                ith_files_DISK+=("${files_array[i]}")
            else
                ith_files_DISK+=("")
            fi
        done
        total_disk_io=()
        for nodeIndex in $(seq 0 $((nodeNumberForDiskIO - 1))); do
            total_disk_io+=($(calculate_total_io_difference "${ith_files_DISK[nodeIndex]}/${expName}_workloadLoad_Before-loading_disk_io_total.txt" "${ith_files_DISK[nodeIndex]}/${expName}-${targetScheme}-Load_workloadLoad_After-flush-compaction_disk_io_total.txt"))
        done
        total_disk_io_kib=("$(sumArray "${total_disk_io[@]}")")
        total_disk_io_gib=$(echo "scale=2; $total_disk_io_kib / 1048576" | bc -l)
        DISKUsageSumList+=($total_disk_io_gib)
        # Network traffic
        total_network_traffic=()
        for nodeIndex in $(seq 0 $((nodeNumberForDiskIO - 1))); do
            total_network_traffic+=($(calculate_network_usage_difference "${ith_files_DISK[nodeIndex]}/${expName}_workloadLoad_Before-loading_network_summary.txt" "${ith_files_DISK[nodeIndex]}/${expName}-${targetScheme}-Load_workloadLoad_After-flush-compaction_network_summary.txt"))
        done
        total_network_traffic_bytes=("$(sumArray "${total_network_traffic[@]}")")
        total_network_traffic_gib=$(echo "scale=2; $total_network_traffic_bytes / 1048576 / 1024" | bc -l)
        NETUsageSumList+=($total_network_traffic_gib)
    done
    # output
    echo -e "\033[1m\033[34m[Resource Usage during Load] scheme: ${targetScheme}, KVNumber: ${KVNumber}, KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"
    echo -e "\033[31;1m95%-percentile CPU Usage (%):\033[0m"
    calculate "${CPUUsageSumList[*]}"
    echo -e "\033[31;1m95%-percentile RAM Usage (GiB):\033[0m"
    calculate "${RAMUsageSumList[*]}"
    echo -e "\033[31;1mTotal Disk I/O (GiB):\033[0m"
    calculate "${DISKUsageSumList[*]}"
    echo -e "\033[31;1mTotal Network traffic (GiB):\033[0m"
    calculate "${NETUsageSumList[*]}"
}

function processResourceResultsForNormalOP {

    declare -A files_by_node=()
    declare -A files_by_workloads=()
    CPUUsageSumList=()
    RAMUsageSumList=()
    DISKUsageSumList=()
    NETUsageSumList=()
    workloads=()
    for file in "${PathToELECTResultSummary}/${targetScheme}"/*; do
        if [[ -d $file ]]; then
            if [[ $file =~ ${expName}-Run-normal-Workload-([^/]+)-KVNumber-${KVNumber}-OPNumber-${OPNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-ClientNumber-${ClientNumber}-Consistency-${ConsistencyLevel}-Node-([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)-Time-[0-9]+ ]]; then
                workload=${BASH_REMATCH[1]}
                nodeID=${BASH_REMATCH[2]}
                files_by_node[$nodeID]+="${file} "
                files_by_workloads[$workload]+="${file} "
            fi
        fi
    done
    for workload in "${!files_by_workloads[@]}"; do
        workloads+=("$workload")
    done
    runningRoundNumber=0
    for nodeID in "${!files_by_node[@]}"; do
        IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
        runningRoundNumber=$((${#files_array[@]} / ${#workloads[@]}))
    done
    for i in $(seq 0 $((runningRoundNumber - 1))); do
        roundLocalCPUUsageList=()
        roundLocalRAMUsageList=()
        roundLocalDISKUsageList=()
        roundLocalNETUsageList=()
        for workload in "${workloads[@]}"; do
            # CPU usage
            declare -a ith_files_CPU=()
            for nodeID in "${!files_by_node[@]}"; do
                IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
                declare -a filtered_files=()
                for file in "${files_array[@]}"; do
                    if [[ $file == *"$workload"* ]]; then
                        filtered_files+=("$file")
                    fi
                done
                if [ $i -lt ${#filtered_files[@]} ]; then
                    ith_files_CPU+=("${filtered_files[i]}/${expName}_Running_cpu_usage.txt")
                else
                    ith_files_CPU+=("")
                fi
            done
            combined_cpu_95th_percentile=$(calculate_combined_cpu_95th_percentile "${ith_files_CPU[@]}")
            roundLocalCPUUsageList+=($combined_cpu_95th_percentile)
            # DRAM usage
            declare -a ith_files_RAM=()
            for nodeID in "${!files_by_node[@]}"; do
                IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
                declare -a filtered_files=()
                for file in "${files_array[@]}"; do
                    if [[ $file == *"$workload"* ]]; then
                        filtered_files+=("$file")
                    fi
                done
                if [ $i -lt ${#filtered_files[@]} ]; then
                    ith_files_RAM+=("${filtered_files[i]}/${expName}_Running_memory_usage.txt")
                else
                    ith_files_RAM+=("")
                fi
            done
            combined_ram_95th_percentile=$(calculate_memory_95th_percentile "${ith_files_RAM[@]}")
            roundLocalRAMUsageList+=($combined_ram_95th_percentile)
            # Disk IO usage
            nodeNumberForDiskIO=${#files_by_node[@]}
            declare -a ith_files_DISK=()
            for nodeID in "${!files_by_node[@]}"; do
                IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
                declare -a filtered_files=()
                for file in "${files_array[@]}"; do
                    if [[ $file == *"$workload"* ]]; then
                        filtered_files+=("$file")
                    fi
                done
                if [ $i -lt ${#filtered_files[@]} ]; then
                    ith_files_DISK+=("${filtered_files[i]}")
                else
                    ith_files_DISK+=("")
                fi
            done
            total_disk_io=()
            for nodeIndex in $(seq 0 $((nodeNumberForDiskIO - 1))); do
                total_disk_io+=($(calculate_total_io_difference "${ith_files_DISK[nodeIndex]}/${expName}_${workload}_Before-run_disk_io_total.txt" "${ith_files_DISK[nodeIndex]}/${expName}_${workload}_After-run_disk_io_total.txt"))
            done
            total_disk_io_kib=("$(sumArray "${total_disk_io[@]}")")
            total_disk_io_gib=$(echo "scale=2; $total_disk_io_kib / 1048576" | bc -l)
            roundLocalDISKUsageList+=($total_disk_io_gib)
            # Network traffic
            total_network_traffic=()
            for nodeIndex in $(seq 0 $((nodeNumberForDiskIO - 1))); do
                total_network_traffic+=($(calculate_network_usage_difference "${ith_files_DISK[nodeIndex]}/${expName}_${workload}_Before-run_network_summary.txt" "${ith_files_DISK[nodeIndex]}/${expName}_${workload}_After-run_network_summary.txt"))
            done
            total_network_traffic_bytes=("$(sumArray "${total_network_traffic[@]}")")
            total_network_traffic_gib=$(echo "scale=2; $total_network_traffic_bytes / 1048576 / 1024" | bc -l)
            roundLocalNETUsageList+=($total_network_traffic_gib)
        done
        # process each round
        average_CPU=$(calculate_average "${roundLocalCPUUsageList[@]}")
        average_RAM=$(calculate_average "${roundLocalRAMUsageList[@]}")
        average_DISK=$(calculate_average "${roundLocalDISKUsageList[@]}")
        average_NET=$(calculate_average "${roundLocalNETUsageList[@]}")
        CPUUsageSumList+=($average_CPU)
        RAMUsageSumList+=($average_RAM)
        DISKUsageSumList+=($average_DISK)
        NETUsageSumList+=($average_NET)
    done

    # output
    echo -e "\033[1m\033[34m[Resource usage with normal operations] scheme: ${targetScheme}, KVNumber: ${KVNumber}, KeySize: ${keylength}, ValueSize: ${fieldlength}, OPNumber: ${OPNumber}\033[0m"
    echo -e "\033[31;1m95%-percentile CPU Usage (%):\033[0m"
    calculate "${CPUUsageSumList[*]}"
    echo -e "\033[31;1m95%-percentile RAM Usage (GiB):\033[0m"
    calculate "${RAMUsageSumList[*]}"
    echo -e "\033[31;1mTotal Disk I/O (GiB):\033[0m"
    calculate "${DISKUsageSumList[*]}"
    echo -e "\033[31;1mTotal Network traffic (GiB):\033[0m"
    calculate "${NETUsageSumList[*]}"
}

function processResourceResultsForDegradedOP {

    declare -A files_by_node=()
    declare -A files_by_workloads=()
    CPUUsageSumList=()
    RAMUsageSumList=()
    DISKUsageSumList=()
    NETUsageSumList=()
    workloads=()
    for file in "${PathToELECTResultSummary}/${targetScheme}"/*; do
        if [[ -d $file ]]; then
            if [[ $file =~ ${expName}-Run-degraded-Workload-([^/]+)-KVNumber-${KVNumber}-OPNumber-${OPNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-ClientNumber-${ClientNumber}-Consistency-${ConsistencyLevel}-Node-([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)-Time-[0-9]+ ]]; then
                workload=${BASH_REMATCH[1]}
                nodeID=${BASH_REMATCH[2]}
                files_by_node[$nodeID]+="${file} "
                files_by_workloads[$workload]+="${file} "
            fi
        fi
    done
    for workload in "${!files_by_workloads[@]}"; do
        workloads+=("$workload")
    done
    runningRoundNumber=0
    for nodeID in "${!files_by_node[@]}"; do
        IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
        runningRoundNumber=$((${#files_array[@]} / ${#workloads[@]}))
    done
    for i in $(seq 0 $((runningRoundNumber - 1))); do
        roundLocalCPUUsageList=()
        roundLocalRAMUsageList=()
        roundLocalDISKUsageList=()
        roundLocalNETUsageList=()
        for workload in "${workloads[@]}"; do
            # CPU usage
            declare -a ith_files_CPU=()
            for nodeID in "${!files_by_node[@]}"; do
                IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
                declare -a filtered_files=()
                for file in "${files_array[@]}"; do
                    if [[ $file == *"$workload"* ]]; then
                        filtered_files+=("$file")
                    fi
                done
                if [ $i -lt ${#filtered_files[@]} ]; then
                    ith_files_CPU+=("${filtered_files[i]}/${expName}_Running_cpu_usage.txt")
                else
                    ith_files_CPU+=("")
                fi
            done
            combined_cpu_95th_percentile=$(calculate_combined_cpu_95th_percentile "${ith_files_CPU[@]}")
            roundLocalCPUUsageList+=($combined_cpu_95th_percentile)
            # DRAM usage
            declare -a ith_files_RAM=()
            for nodeID in "${!files_by_node[@]}"; do
                IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
                declare -a filtered_files=()
                for file in "${files_array[@]}"; do
                    if [[ $file == *"$workload"* ]]; then
                        filtered_files+=("$file")
                    fi
                done
                if [ $i -lt ${#filtered_files[@]} ]; then
                    ith_files_RAM+=("${filtered_files[i]}/${expName}_Running_memory_usage.txt")
                else
                    ith_files_RAM+=("")
                fi
            done
            combined_ram_95th_percentile=$(calculate_memory_95th_percentile "${ith_files_RAM[@]}")
            roundLocalRAMUsageList+=($combined_ram_95th_percentile)
            # Disk IO usage
            nodeNumberForDiskIO=${#files_by_node[@]}
            declare -a ith_files_DISK=()
            for nodeID in "${!files_by_node[@]}"; do
                IFS=' ' read -r -a files_array <<<"${files_by_node[$nodeID]}"
                declare -a filtered_files=()
                for file in "${files_array[@]}"; do
                    if [[ $file == *"$workload"* ]]; then
                        filtered_files+=("$file")
                    fi
                done
                if [ $i -lt ${#filtered_files[@]} ]; then
                    ith_files_DISK+=("${filtered_files[i]}")
                else
                    ith_files_DISK+=("")
                fi
            done
            total_disk_io=()
            for nodeIndex in $(seq 0 $((nodeNumberForDiskIO - 1))); do
                total_disk_io+=($(calculate_total_io_difference "${ith_files_DISK[nodeIndex]}/${expName}_${workload}_Before-run_disk_io_total.txt" "${ith_files_DISK[nodeIndex]}/${expName}_${workload}_After-run_disk_io_total.txt"))
            done
            total_disk_io_kib=("$(sumArray "${total_disk_io[@]}")")
            total_disk_io_gib=$(echo "scale=2; $total_disk_io_kib / 1048576" | bc -l)
            roundLocalDISKUsageList+=($total_disk_io_gib)
            # Network traffic
            total_network_traffic=()
            for nodeIndex in $(seq 0 $((nodeNumberForDiskIO - 1))); do
                total_network_traffic+=($(calculate_network_usage_difference "${ith_files_DISK[nodeIndex]}/${expName}_${workload}_Before-run_network_summary.txt" "${ith_files_DISK[nodeIndex]}/${expName}_${workload}_After-run_network_summary.txt"))
            done
            total_network_traffic_bytes=("$(sumArray "${total_network_traffic[@]}")")
            total_network_traffic_gib=$(echo "scale=2; $total_network_traffic_bytes / 1048576 / 1024" | bc -l)
            roundLocalNETUsageList+=($total_network_traffic_gib)
        done
        # process each round
        average_CPU=$(calculate_average "${roundLocalCPUUsageList[@]}")
        average_RAM=$(calculate_average "${roundLocalRAMUsageList[@]}")
        average_DISK=$(calculate_average "${roundLocalDISKUsageList[@]}")
        average_NET=$(calculate_average "${roundLocalNETUsageList[@]}")
        CPUUsageSumList+=($average_CPU)
        RAMUsageSumList+=($average_RAM)
        DISKUsageSumList+=($average_DISK)
        NETUsageSumList+=($average_NET)
    done

    # output
    echo -e "\033[1m\033[34m[Resource usage with degraded operations] scheme: ${targetScheme}, KVNumber: ${KVNumber}, KeySize: ${keylength}, ValueSize: ${fieldlength}, OPNumber: ${OPNumber}\033[0m"
    echo -e "\033[31;1m95%-percentile CPU Usage (%):\033[0m"
    calculate "${CPUUsageSumList[*]}"
    echo -e "\033[31;1m95%-percentile RAM Usage (GiB):\033[0m"
    calculate "${RAMUsageSumList[*]}"
    echo -e "\033[31;1mTotal Disk I/O (GiB):\033[0m"
    calculate "${DISKUsageSumList[*]}"
    echo -e "\033[31;1mTotal Network traffic (GiB):\033[0m"
    calculate "${NETUsageSumList[*]}"
}

if [ "${outputType}" == "Load" ]; then
    processResourceResultsForLoad
    echo ""
elif [ "${outputType}" == "normal" ]; then
    processResourceResultsForNormalOP
    echo ""
elif [ "${outputType}" == "degraded" ]; then
    processResourceResultsForDegradedOP
    echo ""
fi
