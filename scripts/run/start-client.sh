#!/bin/bash
. /etc/profile
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../common.sh"

kill -9 $(ps aux | grep ycsb | grep -v grep | awk 'NR == 1' | awk {'print $2'})
function setupCluster {
    coordinator=$1
    mode=$2

    cd ${PathToELECTPrototype} || exit

    if [ $mode == "cassandra" ]; then
        echo "Create keyspace ycsbraw;"
        bin/cqlsh "$coordinator" -e "create keyspace ycsbraw WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };
        USE ycsbraw;
        create table usertable0 (y_id varchar primary key, field0 varchar);
        ALTER TABLE usertable0 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': ${SSTableSize}, 'fanout_size': ${LSMTreeFanOutRatio}};
        ALTER TABLE ycsbraw.usertable0 WITH compression = {'enabled':'false'};
        consistency all;"
    else
        echo "Create keyspace ycsb;"
        bin/cqlsh "$coordinator" -e "create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };
        USE ycsb;
        create table usertable0 (y_id varchar primary key, field0 varchar);
        ALTER TABLE usertable0 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': ${SSTableSize}, 'fanout_size': ${LSMTreeFanOutRatio}};
        ALTER TABLE usertable1 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': ${SSTableSize}, 'fanout_size': ${LSMTreeFanOutRatio}};
        ALTER TABLE usertable2 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': ${SSTableSize}, 'fanout_size': ${LSMTreeFanOutRatio}};
        ALTER TABLE ycsb.usertable0 WITH compression = {'enabled':'false'};
        ALTER TABLE ycsb.usertable1 WITH compression = {'enabled':'false'};
        ALTER TABLE ycsb.usertable2 WITH compression = {'enabled':'false'};
        consistency all;"
    fi
}

setupCluster "$1" "$2"

# -e "create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };
#         USE ycsb;
#         create table usertable0 (y_id varchar primary key, field0 varchar);
#         ALTER TABLE usertable0 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 4, 'fanout_size': 10 };
#         ALTER TABLE usertable1 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 4, 'fanout_size': 10 };
#         ALTER TABLE usertable2 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 4, 'fanout_size': 10 };
#         ALTER TABLE ycsb.usertable0 WITH compression = {'enabled':'false'};
#         ALTER TABLE ycsb.usertable1 WITH compression = {'enabled':'false'};
#         ALTER TABLE ycsb.usertable2 WITH compression = {'enabled':'false'};
#         consistency all;"