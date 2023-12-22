#!/bin/bash
. /etc/profile
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../common.sh"
function reload {
    cd ${PathToELECTPrototype} || exit
    bin/nodetool coldStartup reload
}

reload 
