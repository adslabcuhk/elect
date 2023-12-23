#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
setupMode=${1:-"partial"}
kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})

my_ip=$(ip addr show ${networkInterface} | grep 'inet ' | awk '{print $2}' | cut -d/ -f1)
echo "my_ip: ${my_ip}"
tokens=($(generate_tokens))
echo "Target tokens: ${tokens[*]}"
echo "Target nodes: ${NodesList[*]}"

find_ip_index() {
    local given_ip=$1
    local i
    for i in "${!NodesList[@]}"; do
        if [ "${NodesList[$i]}" == "$given_ip" ]; then
            echo $i
            return
        fi
    done
    echo "-1"
}

given_ip=${my_ip}
index=$(find_ip_index $given_ip)

if [ $index -ne -1 ]; then
    echo "This node is the $index node of ELECT cluster, update server configuration"
    selected_token=${tokens[$index]}
    echo "selected_token: ${selected_token}"

    cd ${PathToELECTPrototype} || exit
    sed -i "s/initial_token:.*$/initial_token: ${selected_token}/" ${PathToELECTPrototype}/conf/elect.yaml
    tokens_string=$(
        IFS=,
        echo "${tokens[*]}"
    )
    sed -i "s/token_ranges: \".*\"/token_ranges: ${tokens_string}/" ${PathToELECTPrototype}/conf/elect.yaml
    sed -i "s/rpc_address:.*$/rpc_address: ${my_ip}/" ${PathToELECTPrototype}/conf/elect.yaml
    sed -i "s/listen_address:.*$/listen_address: ${my_ip}/" ${PathToELECTPrototype}/conf/elect.yaml
    sed -i "s/cold_tier_ip:.*$/cold_tier_ip: ${OSSServerNode}/" ${PathToELECTPrototype}/conf/elect.yaml
    sed -i "s/cold_tier_port:.*$/cold_tier_port: ${OSSServerPort}/" ${PathToELECTPrototype}/conf/elect.yaml
    sed -i "s/rpc_address:.*$/rpc_address: ${my_ip}/" ${PathToELECTPrototype}/conf/elect.yaml
    sed -i "s/user_name:.*$/user_name: ${UserName}/" ${PathToELECTPrototype}/conf/elect.yaml
    nodes_string=$(
        IFS=,
        echo "${NodesList[*]}"
    )
    sed -i "s/- seeds: \".*\"/- seeds: \"$nodes_string\"/" ${PathToELECTPrototype}/conf/elect.yaml
    cp ${PathToELECTPrototype}/conf/elect.yaml ${PathToELECTPrototype}/elect.yaml

fi

if [ ${setupMode} == "full" ]; then
    if [ ! -d "${PathToELECTPrototype}/lib" ]; then
        mkdir -p ${PathToELECTPrototype}/lib
    fi
    if [ ! -d "${PathToELECTPrototype}/build" ]; then
        mkdir -p ${PathToELECTPrototype}/build
    fi
    rm -rf data logs
    mkdir -p data/receivedParityHashes/
    mkdir -p data/localParityHashes/
    mkdir -p data/ECMetadata/
    mkdir -p data/tmp/
    mkdir -p logs
    ant realclean && ant -Duse.jdk11=true

    cd ${PathToELECTPrototype}/src/native/src/org/apache/cassandra/io/erasurecode/ || exit
    ./genlib.sh
    rm -rf ${PathToELECTPrototype}/lib/sigar-bin/libec.so
    cp ${PathToELECTPrototype}/src/native/src/org/apache/cassandra/io/erasurecode/libec.so ${PathToELECTPrototype}/lib/sigar-bin

    cd ${PathToYCSB} || exit
    mvn clean package

    cd ${PathToColdTier} || exit
    rm -rf data *.log
    mkdir -p data
    make clean
    make
fi
