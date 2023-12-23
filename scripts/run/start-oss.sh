#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

function startOSSNode {

    kill -9 $(ps aux | grep OSSServer | grep -v grep | awk 'NR == 1' | awk {'print $2'})
    sleep 2
    cd ${PathToArtifact}/src/coldTier || exit

    # rm -rf data OSSLog.txt
    if [ ! -d "data" ]; then
        mkdir -p data
    fi

    nohup java OSSServer ${OSSServerPort} >>${PathToArtifact}/src/coldTier/OSSLog.txt 2>&1 &
}

startOSSNode
