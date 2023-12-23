#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source ${SCRIPT_DIR}/settings.sh

playbookSet=(playbook-load.yaml playbook-run.yaml playbook-flush.yaml playbook-backup.yaml playbook-startup.yaml playbook-fail.yaml playbook-recovery.yaml)

function generate_tokens {
    python3 ${SCRIPT_DIR}/genToken.py ${NodeNumber} >${SCRIPT_DIR}/token.txt
    readarray -t lines <${SCRIPT_DIR}/token.txt
    local -a tokens=()
    for line in "${lines[@]}"; do
        if [[ $line == *"initial_token:"* ]]; then
            token=$(echo $line | grep -oP '(?<=initial_token: )[-0-9]+')
            tokens+=("$token")
        fi
    done
    rm -rf ${SCRIPT_DIR}/token.txt
    echo ${tokens[*]}
}

function setupNodeInfo {
    targetHostInfo=$1
    if [ -f ${SCRIPT_DIR}/exp/${targetHostInfo} ]; then
        rm -rf ${SCRIPT_DIR}/exp/${targetHostInfo}
    fi
    echo "[elect_servers]" >>${SCRIPT_DIR}/exp/${targetHostInfo}
    for ((i = 1; i <= NodeNumber; i++)); do
        echo "server${i} ansible_host=${NodesList[(($i - 1))]}" >>${SCRIPT_DIR}/exp/${targetHostInfo}
    done
    echo >>${SCRIPT_DIR}/exp/${targetHostInfo}
    echo "[elect_oss]" >>${SCRIPT_DIR}/exp/${targetHostInfo}
    echo "oss ansible_host=${OSSServerNode}" >>${SCRIPT_DIR}/exp/${targetHostInfo}
    echo >>${SCRIPT_DIR}/exp/${targetHostInfo}
    echo "[elect_client]" >>${SCRIPT_DIR}/exp/${targetHostInfo}
    echo "client ansible_host=${ClientNode}" >>${SCRIPT_DIR}/exp/${targetHostInfo}
    # random select the failure node from the total node list
    failure_nodes=($(shuf -i 1-${NodeNumber} -n 1))
    echo >>${SCRIPT_DIR}/exp/${targetHostInfo}
    echo "[elect_failure]" >>${SCRIPT_DIR}/exp/${targetHostInfo}
    echo "server${failure_nodes} ansible_host=${NodesList[(($failure_nodes - 1))]}" >>${SCRIPT_DIR}/exp/${targetHostInfo}

    # Setup user ID for each playbook
    for playbook in "${playbookSet[@]}"; do
        if [ ! -f ${playbook} ]; then
            cp ${SCRIPT_DIR}/playbook/${playbook} ${SCRIPT_DIR}/exp/${playbook}
        else
            rm -rf ${SCRIPT_DIR}/exp/${playbook}
            cp ${SCRIPT_DIR}/playbook/${playbook} ${SCRIPT_DIR}/exp/${playbook}
        fi
        sed -i "s/\(become_user: \)".*"/become_user: ${UserName}/" ${SCRIPT_DIR}/exp/${playbook}
        sed -i "s|PATH_TO_ELECT|${PathToArtifact}|g" ${SCRIPT_DIR}/exp/${playbook}
        sed -i "s|PATH_TO_DB_BACKUP|${PathToELECTExpDBBackup}|g" ${SCRIPT_DIR}/exp/${playbook}
    done
}

function treeSizeEstimation {
    KVNumber=$1
    keylength=$2
    fieldlength=$3
    initial_count=${SSTableSize}
    ratio=${LSMTreeFanOutRatio}
    target_count=$((KVNumber * (keylength + fieldlength) / NodeNumber / 1024 / 1024 / 4))

    current_count=$initial_count
    current_level=1

    while [ $current_count -lt $target_count ]; do
        current_count=$((current_count * ratio))
        current_level=$((current_level + 1))
    done
    treeLevels=$((current_level))
    echo ${treeLevels}
}

function dataSizeEstimation {
    KVNumber=$1
    keylength=$2
    fieldlength=$3
    dataSizeOnEachNode=$(echo "scale=2; $KVNumber * ($keylength + $fieldlength) / $NodeNumber / 1024 / 1024 / 1024 * 3" | bc)
    echo ${dataSizeOnEachNode}
}

function initialDelayEstimation {
    dataSizeOnEachNode=$1
    scheme=$2
    if [ "${scheme}" == "cassandra" ]; then
        initialDelay=65536
        echo ${initialDelay}
    else
        initialDellayLocal=$(echo "scale=2; $dataSizeOnEachNode * 8 + 3" | bc)
        initialDellayLocalCeil=$(echo "scale=0; (${initialDellayLocal} + 0.5)/1" | bc)
        echo ${initialDellayLocalCeil}
    fi
}

function waitFlushCompactionTimeEstimation {
    dataSizeOnEachNode=$1
    scheme=$2
    if [ "${scheme}" == "cassandra" ]; then
        waitTime=$(echo "scale=2; $dataSizeOnEachNode * 500" | bc)
        waitTimeCeil=$(echo "scale=0; (${waitTime} + 0.5)/1" | bc)
        echo ${waitTimeCeil}
    else
        waitTime=$(echo "scale=2; ($dataSizeOnEachNode * 1024  / 4 / 3 / ($concurrentEC / 2)) * 80 + $dataSizeOnEachNode * 500 + 240" | bc)
        waitTimeCeil=$(echo "scale=0; (${waitTime} + 0.5)/1" | bc)
        echo ${waitTimeCeil}
    fi
}

function load {
    expName=$1
    targetScheme=$2
    KVNumber=$3
    keylength=$4
    fieldlength=$5
    simulatedClientNumber=$6
    storageSavingTarget=$7
    ecK=$8

    echo "Start loading data into ${targetScheme}"
    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Generate playbook
    setupNodeInfo hosts.ini
    dataSizeOnEachNode=$(dataSizeEstimation ${KVNumber} ${keylength} ${fieldlength})
    initialDelayTime=$(initialDelayEstimation ${dataSizeOnEachNode} ${targetScheme})
    treeLevels=$(treeSizeEstimation ${KVNumber} ${keylength} ${fieldlength})
    # Modify load playbook
    if [ ${targetScheme} == "cassandra" ]; then
        sed -i "s/\(mode: \)".*"/mode: cassandra/" ${SCRIPT_DIR}/exp/playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsbraw/" ${SCRIPT_DIR}/exp/playbook-load.yaml
        sed -i "s/\(treeLevels: \)".*"/treeLevels: 9/" ${SCRIPT_DIR}/exp/playbook-load.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: 65536/" ${SCRIPT_DIR}/exp/playbook-load.yaml
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: 0/" ${SCRIPT_DIR}/exp/playbook-load.yaml
    elif [ ${targetScheme} == "elect" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" ${SCRIPT_DIR}/exp/playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" ${SCRIPT_DIR}/exp/playbook-load.yaml
        sed -i "s/\(treeLevels: \)".*"/treeLevels: ${treeLevels}/" ${SCRIPT_DIR}/exp/playbook-load.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: ${initialDelayTime}/" ${SCRIPT_DIR}/exp/playbook-load.yaml
        sed -i "s/\(target_saving: \)".*"/target_saving: ${storageSavingTarget}/" ${SCRIPT_DIR}/exp/playbook-load.yaml
        sed -i "s/\(data_block_num: \)".*"/data_block_num: ${ecK}/" ${SCRIPT_DIR}/exp/playbook-load.yaml
        sed -i "s/\(parity_block_num: \)".*"/parity_block_num: 2/" ${SCRIPT_DIR}/exp/playbook-load.yaml
    fi

    sed -i "s/\(expName: \)".*"/expName: "${expName}"/" ${SCRIPT_DIR}/exp/playbook-load.yaml
    sed -i "s/\(coordinator: \)".*"/coordinator: "${NodesList[0]}"/" ${SCRIPT_DIR}/exp/playbook-load.yaml
    sed -i "s/record_count:.*$/record_count: ${KVNumber}/" ${SCRIPT_DIR}/exp/playbook-load.yaml
    sed -i "s/key_length:.*$/key_length: ${keylength}/" ${SCRIPT_DIR}/exp/playbook-load.yaml
    sed -i "s/filed_length:.*$/filed_length: ${fieldlength}/" ${SCRIPT_DIR}/exp/playbook-load.yaml
    sed -i "s/\(workload: \)".*"/workload: \"workloadLoad\"/" ${SCRIPT_DIR}/exp/playbook-load.yaml
    sed -i "s/\(threads: \)".*"/threads: ${simulatedClientNumber}/" ${SCRIPT_DIR}/exp/playbook-load.yaml

    ansible-playbook -v -i ${PathToScripts}/exp/hosts.ini ${PathToScripts}/exp/playbook-load.yaml
}

function flush {
    expName=$1
    targetScheme=$2
    waitTime=$3
    echo "Start for flush and wait for compaction of ${targetScheme}"
    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Copy playbook
    setupNodeInfo hosts.ini
    # Modify playbook
    sed -i "s/\(expName: \)".*"/expName: "${ExpName}-${targetScheme}-Load"/" ${PathToScripts}/exp/playbook-flush.yaml
    sed -i "s/\(workload: \)".*"/workload: \"workloadLoad\"/" ${PathToScripts}/exp/playbook-flush.yaml
    sed -i "s/\(seconds: \)".*"/seconds: ${waitTime}/" ${PathToScripts}/exp/playbook-flush.yaml
    ansible-playbook -v -i ${PathToScripts}/exp/hosts.ini ${PathToScripts}/exp/playbook-flush.yaml
}

function backup {
    expName=$1
    targetScheme=$2
    KVNumber=$3
    keylength=$4
    fieldlength=$5
    codingK=$6
    storageSavingTarget=$7

    echo "Start copy data of ${targetScheme} to backup, this will kill the online system!!!"
    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Copy playbook
    setupNodeInfo hosts.ini
    # Modify playbook
    sed -i "s/Scheme/${targetScheme}/g" ${PathToScripts}/exp/playbook-backup.yaml
    sed -i "s/DATAPATH/${expName}-KVNumber-${KVNumber}-KeySize-${keylength}-ValueSize-${fieldlength}/g" ${PathToScripts}/exp/playbook-backup.yaml
    ansible-playbook -v -i ${PathToScripts}/exp/hosts.ini ${PathToScripts}/exp/playbook-backup.yaml
}

function startupFromBackup {
    expName=$1
    targetScheme=$2
    KVNumber=$3
    keylength=$4
    fieldlength=$5

    echo "Start copy data of ${targetScheme} back from backup"
    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Copy playbook
    setupNodeInfo hosts.ini
    # Modify playbook
    sed -i "s/Scheme/${targetScheme}/g" ${PathToScripts}/exp/playbook-startup.yaml
    sed -i "s/DATAPATH/${expName}-KVNumber-${KVNumber}-KeySize-${keylength}-ValueSize-${fieldlength}/g" ${PathToScripts}/exp/playbook-startup.yaml
    ansible-playbook -v -i ${PathToScripts}/exp/hosts.ini ${PathToScripts}/exp/playbook-startup.yaml
}

function failnodes {
    echo "Fail node for degraded test"
    # Copy playbook
    setupNodeInfo hosts.ini
    # Modify playbook
    ansible-playbook -v -i ${PathToScripts}/exp/hosts.ini ${PathToScripts}/exp/playbook-fail.yaml
}

function runExp {
    expName=$1
    targetScheme=$2
    round=$3
    runningType=$4
    KVNumber=$5
    operationNumber=$6
    keylength=$7
    fieldlength=$8
    workload=$9
    shift 9
    simulatedClientNumber=$1
    consistency=$2
    runningMode=$3
    extraFlag=${4:-}

    echo "Start run benchmark to ${targetScheme}, settings: expName is ${expName}, target scheme is ${targetScheme}, running mode is ${runningType}, KVNumber is ${KVNumber}, operationNumber is ${operationNumber}, workload is ${workload}, simulatedClientNumber is ${simulatedClientNumber}, consistency is ${consistency}, running mode is ${runningMode}. ExtraFlag is ${extraFlag}"

    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Normal/Degraded Ops

    echo "Start running $workload on ${targetScheme} round $round"
    setupNodeInfo hosts.ini
    # Modify run palybook
    if [ ${targetScheme} == "cassandra" ]; then
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsbraw/" ${PathToScripts}/exp/playbook-run.yaml
    else
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" ${PathToScripts}/exp/playbook-run.yaml
    fi
    sed -i "s/\(threads: \)".*"/threads: ${simulatedClientNumber}/" ${PathToScripts}/exp/playbook-run.yaml
    sed -i "s/\(workload: \)".*"/workload: \"${workload}\"/" ${PathToScripts}/exp/playbook-run.yaml
    sed -i "s/\(expName: \)".*"/expName: "${ExpName}"/" ${PathToScripts}/exp/playbook-run.yaml
    sed -i "s/record_count:.*$/record_count: ${KVNumber}/" ${PathToScripts}/exp/playbook-run.yaml
    sed -i "s/key_length:.*$/key_length: ${keylength}/" ${PathToScripts}/exp/playbook-run.yaml
    sed -i "s/filed_length:.*$/filed_length: ${fieldlength}/" ${PathToScripts}/exp/playbook-run.yaml
    sed -i "s/operation_count:.*$/operation_count: ${operationNumber}/" ${PathToScripts}/exp/playbook-run.yaml
    sed -i "s/\(consistency: \)".*"/consistency: ${consistency}/" ${PathToScripts}/exp/playbook-run.yaml
    sed -i "s/\(runningMode: \)".*"/runningMode: ${runningMode}/" ${PathToScripts}/exp/playbook-run.yaml
    sed -i "s/\(extraFlag: \)".*"/extraFlag: ${extraFlag}/" ${PathToScripts}/exp/playbook-run.yaml
    if [ "${workload}" == "workloade" ] || [ "${workload}" == "workloadscan" ]; then
        # generate scanNumber = operationNumber / 10
        scanNumber=$((operationNumber / 10))
        sed -i "s/operation_count:.*$/operation_count: ${scanNumber}/" ${PathToScripts}/exp/playbook-run.yaml
    fi
    ansible-playbook -v -i ${PathToScripts}/exp/hosts.ini ${PathToScripts}/exp/playbook-run.yaml
    ## Collect
    ## Collect running logs
    for nodeIP in "${NodesList[@]}"; do
        echo "Copy loading stats of loading for ${expName}-${targetScheme} back, current working on node ${nodeIP}"
        if [ ! -d ${PathToELECTLog}/${targetScheme}/${ExpName}-Load-${nodeIP} ]; then
            mkdir -p ${PathToELECTLog}/${targetScheme}/${ExpName}-Load-${nodeIP}
        fi
        scp -r ${UserName}@${nodeIP}:${PathToELECTLog} ${PathToELECTResultSummary}/${targetScheme}/${ExpName}-Run-Workload-${workload}-ClientNumber-${simulatedClientNumber}-Consistency-${consistency}-Node-${nodeIP}-Time-$(date +%s)
        ssh ${UserName}@${nodeIP} "rm -rf '${PathToELECTLog}'; mkdir -p '${PathToELECTLog}'"
    done

}

function loadDataForEvaluation {
    expName=$1
    scheme=$2
    KVNumber=$3
    keylength=$4
    fieldlength=$5
    simulatedClientNumber=${6:-"${defaultSimulatedClientNumber}"}
    storageSavingTarget=${7:-"0.6"}
    codingK=${8:-"4"}
    extraFlag=${9:-}

    # Gen params
    dataSizeOnEachNode=$(dataSizeEstimation ${KVNumber} ${keylength} ${fieldlength})
    initialDelayTime=$(initialDelayEstimation ${dataSizeOnEachNode} ${scheme})
    waitFlushCompactionTime=$(waitFlushCompactionTimeEstimation ${dataSizeOnEachNode} ${scheme})
    treeLevels=$(treeSizeEstimation ${KVNumber} ${keylength} ${fieldlength})

    # Outpout params
    echo "Start experiment to Loading ${scheme}, expName is ${expName}; KVNumber is ${KVNumber}, keylength is ${keylength}, fieldlength is ${fieldlength}, simulatedClientNumber is ${simulatedClientNumber}, storageSavingTarget is ${storageSavingTarget}, codingK is ${codingK}, extraFlag is ${extraFlag}. Estimation of data size on each node is ${dataSizeOnEachNode} GiB, initial delay is ${initialDelayTime}, flush and compaction wait time is ${waitFlushCompactionTime}."
    echo ""
    estimatedTotalRunningTime=$(((${waitFlushCompactionTime} + ${KVNumber} / 20000) / 60))
    echo "The total running time is estimated to be ${estimatedTotalRunningTime} minutes."

    # Load
    load "${expName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}" "${simulatedClientNumber}" "${storageSavingTarget}" "${codingK}"
    flush "${expName}" "${scheme}" "${waitFlushCompactionTime}"
    backup "${expName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}" "${codingK}" "${storageSavingTarget}"

    ## Collect load logs
    for nodeIP in "${NodesList[@]}"; do
        echo "Copy loading stats of loading for ${expName}-${targetScheme} back, current working on node ${nodeIP}"
        if [ ! -d ${PathToELECTLog}/${targetScheme}/${ExpName}-Load-${nodeIP} ]; then
            mkdir -p ${PathToELECTLog}/${targetScheme}/${ExpName}-Load-${nodeIP}
        fi
        scp -r ${UserName}@${nodeIP}:${PathToELECTLog} ${PathToELECTResultSummary}/${targetScheme}/${ExpName}-Load-${nodeIP}-Time-$(date +%s)
        ssh ${UserName}@${nodeIP} "rm -rf '${PathToELECTLog}'; mkdir -p '${PathToELECTLog}'"
        ssh ${UserName}@${OSSServerNode} "du -s --bytes ${PathToColdTier}/data > ${PathToELECTLog}/${ExpName}-${targetScheme}-OSSStorage.log"
        scp ${UserName}@${OSSServerNode}:${PathToELECTLog}/${ExpName}-${targetScheme}-OSSStorage.log ${PathToELECTResultSummary}/${targetScheme}/${ExpName}-OSSStorage.log
    done
}

function doEvaluation {
    expName=$1
    scheme=$2
    KVNumber=$3
    keylength=$4
    fieldlength=$5
    operationNumber=$6
    simulatedClientNumber=$7
    RunningRoundNumber=$8
    runningMode=$9
    shift 9
    workload=$1
    readConsistency=${2}
    extraFlag=${3:-}

    # Outpout params
    echo "Start experiment to Running ${scheme}, expName is ${expName}; KVNumber is ${KVNumber}, keylength is ${keylength}, fieldlength is ${fieldlength}, operationNumber is ${operationNumber}, simulatedClientNumber is ${simulatedClientNumber}, running type is ${runningMode}, workload is ${workload}. The experiment will run ${RunningRoundNumber} rounds. The read consistency level is ${readConsistency}."

    for ((round = 1; round <= RunningRoundNumber; round++)); do
        if [ "${runningMode}" == "normal" ]; then
            startupFromBackup "${expName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}"
            runExp "${expName}" "${scheme}" "${round}" "normal" "${KVNumber}" "${operationNumber}" "${keylength}" "${fieldlength}" "${workload}" "${simulatedClientNumber}" "${readConsistency}" "${runningMode}" "${extraFlag}"
        elif [ "${runningMode}" == "degraded" ]; then
            startupFromBackup "${expName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}"
            failnodes
            runExp "${expName}" "${scheme}" "${round}" "degraded" "${KVNumber}" "${operationNumber}" "${keylength}" "${fieldlength}" "${workload}" "${simulatedClientNumber}" "${readConsistency}" "${runningMode}" "${extraFlag}"
        fi
    done
}

function recovery {
    expName=$1
    targetScheme=$2
    recoveryNode=$3
    KVNumber=$4
    runningRound=${5,-"1"}

    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Copy playbook
    setupNodeInfo hosts.ini

    if [ ${targetScheme} == "cassandra" ]; then
        sed -i "s/\(mode: \)".*"/mode: raw/" playbook-recovery.yaml
    else
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-recovery.yaml
    fi
    sed -i "s/\(seconds: \)".*"/seconds: 900/" playbook-recovery.yaml

    ansible-playbook -v -i ${PathToScripts}/exp/hosts.ini ${PathToScripts}/exp/playbook-recovery.yaml

    echo "Copy running logs of $targetScheme back form $recoveryNode"
    scp -r ${UserName}@${recoveryNode}:${PathToELECTPrototype}/logs/recovery.log ${PathToELECTResultSummary}/${targetScheme}/${expName}-Size-${KVNumber}-recovery-Round-${runningRound}-RecoverNode-${recoveryNode}-Time--$(date +%s).log
}
