#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/common.sh"

while true; do
    if ! ps aux | grep "Exp0-simpleOverall.sh" | grep -v grep >/dev/null; then
        echo "Start running rest of experiments..."
        cleanUp
        bash ${SCRIPT_DIR}/exp/Exp1-ycsb.sh
        cleanUp
        bash ${SCRIPT_DIR}/exp/Exp2-operations.sh
        cleanUp
        bash ${SCRIPT_DIR}/exp/Exp3-breakdown.sh
        cleanUp
        bash ${SCRIPT_DIR}/exp/Exp4-recovery.sh
        cleanUp
        bash ${SCRIPT_DIR}/exp/Exp5-resource.sh
        cleanUp
        bash ${SCRIPT_DIR}/exp/Exp6-kvSize.sh
        cleanUp
        bash ${SCRIPT_DIR}/exp/Exp7-balanceParam.sh
        cleanUp
        bash ${SCRIPT_DIR}/exp/Exp8-ecParam.sh
        cleanUp
        bash ${SCRIPT_DIR}/exp/Exp9-consistency.sh
        cleanUp
        bash ${SCRIPT_DIR}/exp/Exp10-clients.sh
    fi
    echo "Waiting for the Exp0-simpleOverall.sh to finish..."
    sleep 60
done
