#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
function restartNode {
    configureFilePath=$1
    sourceDataDir=$2

    kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})

    echo "Copy DB data back from ${sourceDataDir} to ${PathToELECTPrototype}/data"

    if [ ! -d "${sourceDataDir}" ]; then
        echo "The backup data in ${sourceDataDir} does not exist"
        exit
    fi

    cd "${PathToELECTPrototype}" || exit

    rm -rf data
    cp -r ${sourceDataDir} data
    chmod -R 775 data

    if [ -f conf/elect.yaml ]; then
        echo "Remove old conf/elect.yaml"
        rm conf/elect.yaml
    fi

    echo "Copy DB configuration back from ${sourceDataDir} to ${PathToELECTPrototype}/data"
    cp ${configureFilePath} conf/elect.yaml

    rm -rf logs
    mkdir -p logs

    nohup bin/elect >logs/debug.log 2>&1 &
}

restartNode "$1" "$2"
