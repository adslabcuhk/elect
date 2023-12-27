#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

expName=$1
targetScheme=$2
KVNumber=$3
keylength=$4
fieldlength=$5
codingK=${6:-4}
storageSavingTarget=${7:-0.6}

# fetch hot-tier
hotTier_storage_usage_values=()
for currentIP in "${NodesList[@]}"; do
    # Fetch the storage usage
    HotTierStoragePath="${PathToELECTResultSummary}/${targetScheme}/${expName}-Load-KVNumber-${KVNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-CodingK-${codingK}-Saving-${storageSavingTarget}-Node-${currentIP}-*/${expName}-${targetScheme}-Load_workloadLoad_After-flush-compaction_db_stats.txt"
    for file in $HotTierStoragePath; do
        if [ -f "$file" ]; then
            # echo "Processing: $file"
            while IFS= read -r line; do
                hotTier_storage_usage_values+=("$line")
            done < <(awk '/Total storage usage:/{getline; print}' "$file")
            break; # read only one file for each node
        fi
    done
done

hotTierStorage=0
for value in "${hotTier_storage_usage_values[@]}"; do
    hotTierStorage=$((hotTierStorage + value))
done
hotTierStorage=$(echo "$hotTierStorage / 1073741824" | bc -l)

# fetch cold-tier
file_path=${PathToELECTResultSummary}/${targetScheme}/${expName}-${targetScheme}-KVNumber-${KVNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-CodingK-${codingK}-Saving-${storageSavingTarget}-OSSStorage.log
# Check if the file exists
if [ ! -f "$file_path" ]; then
    echo "OSSStorage log ($file_path) not found!"
    exit 1
fi

coldTierStorage=0
while IFS= read -r line; do
    # Extract the number of bytes using awk
    bytes=$(echo $line | awk '{print $1}')

    # Convert bytes to GiB
    gib=$(echo "$bytes / 1073741824" | bc -l)
    coldTierStorage=$gib
done <"$file_path"

# fetch total storage overhead
coldTierStorageFormatted=$(printf "%.2f" $coldTierStorage)
hotTierStorageFormatted=$(printf "%.2f" $hotTierStorage)

echo -e "\033[1m\033[34m[Exp info] Scheme: ${targetScheme}, KVNumber: ${KVNumber}, KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"
if [ "$targetScheme" == "elect" ]; then
    echo -e "\033[31;1mTotal storage overhead (unit: GiB): $(echo "$coldTierStorageFormatted + $hotTierStorageFormatted" | bc -l)\033[0m"
    echo "Hot-tier storage overhead (unit: GiB): $hotTierStorageFormatted"
    echo "Cold-tier storage overhead (unit: GiB): $coldTierStorageFormatted"
else
    echo -e "\033[31;1mTotal storage overhead (unit: GiB): $hotTierStorageFormatted\033[0m"
fi
echo ""
