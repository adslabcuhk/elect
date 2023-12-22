#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

function startServerNode {

    kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})
    sleep 2
    treeLevels=$1
    initialDelay=$2
    targetStorageSaving=$3
    dataBlockNum=$4
    parityBlockNum=$5
    mode=$6

    echo "treeLevels: ${treeLevels}, initialDelay: ${initialDelay}, targetStorageSaving: ${targetStorageSaving}, dataBlockNum: ${dataBlockNum}, parityBlockNum: ${parityBlockNum}, mode: ${mode}"

    cd ${PathToELECTPrototype}
    if [ ! -f conf/elect.yaml ]; then
        if [ ! -f ${PathToELECTPrototype}/elect.yaml ]; then
            echo "No elect.yaml or elect.yaml configuration file found, error"
            exit
        else
            echo "Backup of elect.yaml not exist, maybe the setup is not correct"
            cp ${PathToELECTPrototype}/elect.yaml ${PathToELECTPrototype}/conf/elect.yaml
        fi
    fi

    rm -rf data logs
    mkdir -p data/receivedParityHashes/
    mkdir -p data/localParityHashes/
    mkdir -p data/ECMetadata/
    mkdir -p data/tmp/
    mkdir -p logs

    # varify mode
    if [ "${mode}" = "cassandra" ]; then
        echo "Running scheme is cassandra"
        sed -i "s/enable_migration:.*$/enable_migration: false/" conf/elect.yaml
        sed -i "s/enable_erasure_coding:.*$/enable_erasure_coding: false/" conf/elect.yaml
    else
        echo "Running scheme is elect"
        sed -i "s/enable_migration:.*$/enable_migration: true/" conf/elect.yaml
        sed -i "s/enable_erasure_coding:.*$/enable_erasure_coding: true/" conf/elect.yaml
        sed -i "s/target_storage_saving:.*$/target_storage_saving: ${targetStorageSaving}/" conf/elect.yaml
        sed -i "s/ec_data_nodes:.*$/ec_data_nodes: ${dataBlockNum}/" conf/elect.yaml
        sed -i "s/parity_nodes:.*$/parity_nodes: ${parityBlockNum}/" conf/elect.yaml
        sed -i "s/max_level_count:.*$/max_level_count: ${treeLevels}/" conf/elect.yaml
        sed -i "s/initial_delay:.*$/initial_delay: ${initialDelay}/" conf/elect.yaml
        sed -i "s/concurrent_ec:.*$/concurrent_ec: ${concurrentEC}/" conf/elect.yaml
        sendSSTables=$((concurrentEC / 2))
        sed -i "s/max_send_sstables:.*$/max_send_sstables: ${sendSSTables}/" conf/elect.yaml
    fi

    nohup bin/elect >logs/debug.log 2>&1 &
}

startServerNode "$1" "$2" "$3" "$4" "$5" "$6"
