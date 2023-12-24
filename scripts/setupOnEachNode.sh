#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/settings.sh"
setupMode=${1:-"partial"}
if [ ${setupMode} == "full" ]; then
    # SSH key-free connection from control node to all nodes
    for nodeIP in "${NodesList[@]}" "${OSSServerNode}" "${ClientNode}"; do
        ssh-keyscan -H ${nodeIP} >>~/.ssh/known_hosts
    done
    if [ ! ${UserName} == "cc" ]; then
        # SSH keygen on control node
        if [ ! -f ~/.ssh/id_rsa ]; then
            ssh-keygen -q -t rsa -b 2048 -N "" -f ~/.ssh/id_rsa
        fi

        for nodeIP in "${NodesList[@]}" "${OSSServerNode}" "${ClientNode}"; do
            echo "Set SSH key-free connection to node ${nodeIP}"
            ssh-copy-id -i ~/.ssh/id_rsa.pub ${UserName}@${nodeIP}
        done
    fi

    # Install packages
    if [ ! -z "${sudoPasswd}" ]; then
        printf ${sudoPasswd} | sudo -S apt-get update
        printf ${sudoPasswd} | sudo -S apt-get install -y ant ant-optional maven clang llvm python3 ansible python3-pip libisal-dev openjdk-11-jdk openjdk-11-jre
        # Disable automatically time setup
        if systemctl is-active --quiet ntpd; then
            echo "Stopping ntpd..."
            printf ${sudoPasswd} | sudo -S systemctl stop ntpd
            printf ${sudoPasswd} | sudo -S systemctl disable ntpd
        else
            echo "ntpd is not active."
        fi

        if systemctl is-active --quiet chronyd; then
            echo "Stopping chronyd..."
            printf ${sudoPasswd} | sudo -S systemctl stop chronyd
            printf ${sudoPasswd} | sudo -S systemctl disable chronyd
        else
            echo "chronyd is not active."
        fi

        if systemctl is-active --quiet systemd-timesyncd; then
            echo "Stopping systemd-timesyncd..."
            printf ${sudoPasswd} | sudo -S systemctl stop systemd-timesyncd
            printf ${sudoPasswd} | sudo -S systemctl disable systemd-timesyncd
        else
            echo "systemd-timesyncd is not active."
        fi
        TIME=$(curl -s "http://worldtimeapi.org/api/timezone/Etc/UTC" | jq -r '.datetime' | cut -d'.' -f1 | tr 'T' ' ')
        printf ${sudoPasswd} | sudo -S timedatectl set-time "$TIME"
    else
        sudo apt-get update
        sudo apt-get install -y ant ant-optional maven clang llvm python3 ansible python3-pip libisal-dev openjdk-11-jdk openjdk-11-jre
        # Disable automatically time setup
        if systemctl is-active --quiet ntpd; then
            echo "Stopping ntpd..."
            sudo systemctl stop ntpd
            sudo systemctl disable ntpd
        else
            echo "ntpd is not active."
        fi

        if systemctl is-active --quiet chronyd; then
            echo "Stopping chronyd..."
            sudo systemctl stop chronyd
            sudo systemctl disable chronyd
        else
            echo "chronyd is not active."
        fi

        if systemctl is-active --quiet systemd-timesyncd; then
            echo "Stopping systemd-timesyncd..."
            sudo systemctl stop systemd-timesyncd
            sudo systemctl disable systemd-timesyncd
        else
            echo "systemd-timesyncd is not active."
        fi
        TIME=$(curl -s "http://worldtimeapi.org/api/timezone/Etc/UTC" | jq -r '.datetime' | cut -d'.' -f1 | tr 'T' ' ')
        sudo timedatectl set-time "$TIME"
    fi

    pip install cassandra-driver numpy scipy
    rm -rf ${PathToELECTLog}
    rm -rf ${PathToELECTExpDBBackup}
fi

# Java configuration
export _JAVA_OPTIONS='-Xmx12g -Xms2048m -XX:MaxDirectMemorySize=2048m'

if [ ! -d "${PathToELECTExpDBBackup}" ]; then
    mkdir -p ${PathToELECTExpDBBackup}
fi

if [ ! -d "${PathToELECTLog}" ]; then
    mkdir -p ${PathToELECTLog}
fi

if [ ! -d "${PathToELECTResultSummary}" ]; then
    mkdir -p ${PathToELECTResultSummary}
fi

bash ${SCRIPT_DIR}/run/update-server.sh ${setupMode}
