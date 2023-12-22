#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
func() {
    record_count=$1
    key_length=$2
    field_length=$3
    threads=$4
    workload=$5
    expName=$6
    keyspace=$7

    cd ${PathToYCSB} || exit

    sed -i "s/recordcount=.*$/recordcount=${record_count}/" workloads/"${workload}"
    sed -i "s/keylength=.*$/keylength=${key_length}/" workloads/"${workload}"
    sed -i "s/fieldlength=.*$/fieldlength=${field_length}/" workloads/"${workload}"
    mode=""
    if [ "$keyspace" == "ycsb" ]; then
        mode="elect"
    else
        mode="cassandra"
    fi

    file_name="${expName}-Load-Scheme-${mode}-${workload}-KVNumber-${record_count}-KeySize-${key_length}-ValueSize-${field_length}-ClientNumber-${threads}-Time-$(date +%s)"

    bin/ycsb.sh load cassandra-cql -p hosts=${NodesList} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -threads ${threads} -s -P workloads/"${workload}" >${PathToELECTResultSummary}/${file_name}.log 2>&1
}

func "$1" "$2" "$3" "$4" "$5" "$6" "$7"

# bin/ycsb.sh load cassandra-cql -p hosts=10.31.0.185 -p cassandra.keyspace=ycsbraw -p cassandra.tracing="false" -threads 32 -s -P workloads/"workloadLoad
