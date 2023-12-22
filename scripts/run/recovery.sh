#!/bin/bash
. /etc/profile
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../common.sh"
function runRecovery {
    mode=$1
    cd ${PathToELECTPrototype}
    if [ $mode == "cassandra" ]; then
        echo "Recovery data for cassandra;"
        kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})
        echo "Target recovery data size of cassandra:" >> logs/recovery.log
        du -s data/data/ycsbraw/ >> logs/recovery.log
        rm -rf data/data/ycsbraw/

        nohup bin/elect >logs/debug.log 2>&1 &

        sleep 60

        start_time=$(($(date +%s%N) / 1000000))
        bin/nodetool repair ycsbraw -j 16 >> logs/recovery.log
        end_time=$(($(date +%s%N) / 1000000))
        elapsed_time=$((end_time - start_time))
        echo "Repair Time Cost: $elapsed_time (ms)" >> logs/recovery.log
    else
        echo "Recovery data for elect/mslm;"
        bin/nodetool recovery ycsb usertable0
    fi
}

runRecovery "$1"
