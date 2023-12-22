#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

func() {

    echo "Backup DB data from $1 to $2"
    sourceDataDir=$1
    targetDataDir=$2
    configureFilePath=$3

    rm -rf "${targetDataDir}"
    if [ ! -d "${targetDataDir}" ]; then
        mkdir -p "${targetDataDir}"
    fi

    if [ ! -d "${sourceDataDir}" ]; then
        echo "Target ${sourceDataDir} does not exist"
        exit
    fi

    cd ${PathToELECTPrototype} || exit

    bin/nodetool coldStartup backup

    sleep 10

    bin/nodetool drain

    sleep 10

    kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})

    cp -r "${sourceDataDir}" "${targetDataDir}"
    cp -r "${configureFilePath}" "${targetDataDir}"
}

func "$1" "$2" "$3"
