#!/bin/bash
. /etc/profile
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../common.sh"
func() {

    recordcount=$1
    keylength=$2
    valuelength=$3
    threads=$4
    workload=$5
    expName=$6
    stage=$7

    dirName="${expName}-Load-${workload}-KVNumber-${recordcount}-KeySize-${keylength}-ValueSize-${valuelength}-ClientNumber-${threads}-Stage-${stage}$(date +%s)-Log"

    cp -r ${PathToELECTPrototype}/logs ${PathToELECTLog}/$dirName
}

func "$1" "$2" "$3" "$4" "$5" "$6" "$7"
