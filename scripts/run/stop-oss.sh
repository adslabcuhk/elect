#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

function stopOSSNode {

    kill -9 $(ps aux | grep FileServer | grep -v grep | awk 'NR == 1' | awk {'print $2'})
}

stopOSSNode
