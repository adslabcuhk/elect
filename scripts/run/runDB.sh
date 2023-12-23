#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

echo "Check failed nodes"
# Variable to store the IPs under [elect_failure]
failure_ips=()
# Flag to indicate whether the current section is [elect_failure]
in_failure_section=false
while read -r line; do
    if [[ "$line" == "[elect_failure]" ]]; then
        in_failure_section=true
        continue
    fi
    if [[ "$in_failure_section" = true && "$line" == \[*\] ]]; then
        break
    fi
    if [[ "$in_failure_section" = true ]]; then
        ip=$(echo "$line" | awk -F'=' '{print $2}')
        failure_ips+=("$ip")
    fi
done <"${PathToScripts}/exp/hosts.ini"
echo "Failed IPs: ${failure_ips[*]}"
copied_list=("${NodesList[@]}")
# Remove the failure IPs from the ip_list
for fail_ip in "${failure_ips[@]}"; do
    for i in "${!copied_list[@]}"; do
        if [[ "${copied_list[$i]}" = "$fail_ip" ]]; then
            unset 'copied_list[i]'
        fi
    done
done
# Optionally, re-index the array
ip_list=("${copied_list[@]}")
# Print the resulting list
echo "Remaining IPs: ${ip_list[*]}"

recordcount=$1
operationcount=$2
key_length=$3
field_length=$4
threads=$5
workload=$6
expName=$7
keyspace=$8
consistency=$9
shift 9
runningMode=$1
extraFlag=${2:-}

cd ${PathToYCSB} || exit

sed -i "s/recordcount=.*$/recordcount=${recordcount}/" workloads/"${workload}"
sed -i "s/operationcount=.*$/operationcount=${operationcount}/" workloads/"${workload}"
sed -i "s/keylength=.*$/keylength=${key_length}/" workloads/"${workload}"
sed -i "s/fieldlength=.*$/fieldlength=${field_length}/" workloads/"${workload}"

mode="cassandra"
if [ "$keyspace" == "ycsb" ]; then
    mode="elect"
fi

file_name="${expName}-Run-${runningMode}-Scheme-${mode}-${workload}-KVNumber-${recordcount}-OPNumber-${operationcount}-KeySize-${key_length}-ValueSize-${field_length}-ClientNumber-${threads}-Consistency-${consistency}-Time-$(date +%s)"

if [ ! -z "${extraFlag}" ]; then
    file_name="${expName}-Run-${runningMode}-Scheme-${mode}-${workload}-KVNumber-${recordcount}-OPNumber-${operationcount}-KeySize-${key_length}-ValueSize-${field_length}-ClientNumber-${threads}-Consistency-${consistency}-ExtraFlag-${extraFlag}-Time-$(date +%s)"
fi

bin/ycsb.sh run cassandra-cql -p hosts=${ip_list} -p cassandra.readconsistencylevel=${consistency} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -threads $threads -s -P workloads/"${workload}" >${PathToELECTResultSummary}/"${file_name}".log 2>&1
