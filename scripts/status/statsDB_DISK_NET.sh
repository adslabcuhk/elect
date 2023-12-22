#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../settings.sh"

function statsDisk_Network_DB {
    expName=$1
    workload=$2
    stage=$3
    # Network interface (change this to your interface, e.g., wlan0, ens33, etc.)
    INTERFACE=${networkInterface}

    # File to store the results
    NET_OUTPUT_FILE="${PathToELECTLog}/${expName}_${workload}_${stage}_network_summary.txt"

    # Extract the received (RX) and transmitted (TX) bytes for the specified interface
    RX_BYTES=$(cat /proc/net/dev | grep $INTERFACE | awk '{print $2}')
    TX_BYTES=$(cat /proc/net/dev | grep $INTERFACE | awk '{print $10}')

    # Write the results to the file
    echo "Summary for interface: $INTERFACE at stage $stage" >>$NET_OUTPUT_FILE
    echo "Bytes received: $RX_BYTES" >>$NET_OUTPUT_FILE
    echo "Bytes sent: $TX_BYTES" >>$NET_OUTPUT_FILE

    # Mount point
    MOUNT_POINT="/"

    # File to store the results
    IO_OUTPUT_FILE="${PathToELECTLog}/${expName}_${workload}_${stage}_disk_io_total.txt"

    # Get the device associated with the mount point (e.g., sda, sdb)
    DEVICE=$(df --output=source "$MOUNT_POINT" | tail -1 | awk -F'/' '{print $NF}')

    # Extract the read and write statistics for the device from /proc/diskstats
    # Fields:
    # 3 - Device name
    # 6 - Number of sectors read
    # 10 - Number of sectors written
    SECTOR_SIZE=512 # Default sector size in bytes
    STATS=$(grep "$DEVICE " /proc/diskstats | awk -v size=$SECTOR_SIZE '{print $3 " " $6*size/1024 " " $10*size/1024}')

    DEVICE_NAME=$(echo "$STATS" | awk '{print $1}')
    TOTAL_KB_READ=$(echo "$STATS" | awk '{print $2}')
    TOTAL_KB_WRITTEN=$(echo "$STATS" | awk '{print $3}')

    # Write the results to the file
    echo "Summary for device: $DEVICE_NAME (mounted at $MOUNT_POINT) at stage: $stage" >>$IO_OUTPUT_FILE
    echo "Total KiB read: $TOTAL_KB_READ" >>$IO_OUTPUT_FILE
    echo "Total KiB written: $TOTAL_KB_WRITTEN" >>$IO_OUTPUT_FILE

    # File to store the results
    DB_OUTPUT_FILE="${PathToELECTLog}/${expName}_${workload}_${stage}_db_stats.txt"
    echo "Record DB status for ${expName} at stage ${stage}" >>"$DB_OUTPUT_FILE"
    # Write the results to the file
    cd ${PathToELECTPrototype} || exit
    bin/nodetool breakdown >>"$DB_OUTPUT_FILE"
    bin/nodetool tpstats >>"$DB_OUTPUT_FILE"
    bin/nodetool tablehistograms ycsb.usertable0 >>"$DB_OUTPUT_FILE"
    bin/nodetool tablehistograms ycsb.usertable1 >>"$DB_OUTPUT_FILE"
    bin/nodetool tablehistograms ycsb.usertable2 >>"$DB_OUTPUT_FILE"
    echo "Total storage usage:" >>"$DB_OUTPUT_FILE"
    du -s --bytes ${PathToELECTPrototype}/data/ | awk '{print $1}' >>"$DB_OUTPUT_FILE"
    echo "LSM-tree:" >>"$DB_OUTPUT_FILE"
    du -s --bytes ${PathToELECTPrototype}/data/data/ | awk '{print $1}' >>"$DB_OUTPUT_FILE"
    echo "Received parity:" >>"$DB_OUTPUT_FILE"
    du -s --bytes ${PathToELECTPrototype}/data/receivedParityHashes/ | awk '{print $1}' >>"$DB_OUTPUT_FILE"
    echo "Local parity:" >>"$DB_OUTPUT_FILE"
    du -s --bytes ${PathToELECTPrototype}/data/localParityHashes/ | awk '{print $1}' >>"$DB_OUTPUT_FILE"
    echo "EC Metadata:" >>"$DB_OUTPUT_FILE"
    du -s --bytes ${PathToELECTPrototype}/data/ECMetadata/ | awk '{print $1}' >>"$DB_OUTPUT_FILE"

    find ${PathToELECTPrototype}/data/data/**/usertable* -type f \( -name "*Index.db" -o -name "*Digest.crc32" -o -name "*Filter.db" -o -name "*Statistics.db" -o -name "*Summary.db" -o -name "*TOC.txt" -o -name "*EC.db" -o -name "*Data.db" \) -print0 | xargs -0 du -s | awk '{
        if($2 ~ /Index\.db/) { sum_index += $1 }
        else if($2 ~ /Digest\.crc32/) { sum_digest += $1 }
        else if($2 ~ /Filter\.db/) { sum_filter += $1 }
        else if($2 ~ /Statistics\.db/) { sum_statistics += $1 }
        else if($2 ~ /Summary\.db/) { sum_summary += $1 }
        else if($2 ~ /TOC\.txt/) { sum_toc += $1 }
        else if($2 ~ /Data\.db/) { sum_data += $1 }
        else if($2 ~ /EC\.db/) { sum_ec += $1 }
        } 
        END {
            print "Total size of Index.db: " sum_index
            print "Total size of Digest.crc32: " sum_digest
            print "Total size of Filter.db: " sum_filter
            print "Total size of Statistics.db: " sum_statistics
            print "Total size of Summary.db: " sum_summary
            print "Total size of TOC.txt: " sum_toc
            print "Total size of Data.db: " sum_data
            print "Total size of EC.db: " sum_ec
            print "Total Metadata Size (In LSM-tree): " sum_index+sum_digest+sum_filter+sum_statistics        +sum_summary+sum_toc
            print "Total Data Size (In LSM-tree): " sum_data+sum_ec
        }' >>"$DB_OUTPUT_FILE"
}

statsDisk_Network_DB "$1" "$2" "$3"
