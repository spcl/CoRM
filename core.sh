#!/bin/bash

#                                                                                                      
# CoRM: Compactable Remote Memory over RDMA
# 
# Help functions to deploy CoRM
#
# Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#



define(){ IFS='\n' read -r -d '' ${1} || true; }
redirection=( "> out" "2> err" "< /dev/null" )

declare -A processPids

__count_process=0
__corm=""


WORKDIR="$PWD"
LOCALWORKDIR="$PWD"
__VERBOSE=1

function log () {
    if [[ $__VERBOSE -ge 1 ]]; then
        echo -e "$@"
    fi
}

function debug () {
    if [[ $__VERBOSE -ge 2 ]]; then
        echo -e "$@"
    fi
}
 
scpFileTo(){
    local server="$1"
    local filename="$2"
    local cmd=( "scp" "$2" "$USER@$server:${WORKDIR}/" )
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

scpFileFrom(){
    local server="$1"
    local filename="$2"
    local cmd=("scp" "$USER@$server:${WORKDIR}/$2" ./)
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

sshCommandAsync() {
    local server=$1
    local command=$2
    local valredirect="${redirection[@]}"
    if ! [[ -z $3 ]]
    then
        valredirect="> "$3" 2>/dev/null"
    fi
    local cmd=( "ssh" "-oStrictHostKeyChecking=no" "$USER@$server" "nohup" "$command" "$valredirect" "&" "echo \$!" )
    local pid=$("${cmd[@]}")
    echo "$pid"
}

sshCommandSync() {
    local server="$1"
    local command="$2"
    local valredirect="${redirection[@]}"
    if ! [[ -z $3 ]]
    then
        valredirect="> "$3" 2>/dev/null"
    fi
    local cmd=( "ssh" "-oStrictHostKeyChecking=no" "$USER@$server" "$command" "$valredirect" )
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

sshKillCommand() {
    local server=$1
    local pid=$2
    cmd=( "ssh" "$USER@$server" "kill -9" "${pid}" )
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

sshStopCommand() {
    local server=$1
    local pid=$2
    cmd=( "ssh" "$USER@$server" "kill -2" "${pid}" )
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

startCorm(){
    local server=$1
    local params=$2
    local pid=$(sshCommandAsync $server "${WORKDIR}/server --server=$server ${params}")
    log "\tCorm is started at ${server} with PID$pid and params ${params}"

    __corm="$server,$pid"
    __corm_server="$server"
}

loadCorm(){
    local size=$1
    local num=$2
    local comm="$WORKDIR/load --server=${__corm_server} --num=$num --size=$size --threads=8 --randomize"
    log "\tStart loading server with $num elements with user size: $size"
    ${comm} 
    log "\t Loading is done"
}

unloadCorm(){
    local num=$1
    local comm="$WORKDIR/unload --server=${__corm_server} --num=$num "
    log "\tStart unloading server with $num elements"
    ${comm} 
    log "\t unLoading is done"
}

killCorm(){
    local servername=$( echo $__corm | cut -d, -f1)
    local pid=$( echo $__corm | cut -d, -f2)
    sshKillCommand $servername $pid
    log "\tCorm is killed at $servername"
}

stopCorm(){
    local servername=$( echo $__corm | cut -d, -f1)
    local pid=$( echo $__corm | cut -d, -f2)
    sshStopCommand $servername $pid
    log "\tCorm is stoppped at $servername"
}

killAllProcesses(){
    echo "try to kill ${__count_process} processes"
    echo "the dict has ${!processPids[@]} entries"
    for id in "${!processPids[@]}"
    do  
        local temp=${processPids[$id]}
        local servername=$( echo $temp | cut -d, -f1)
        local pid=$( echo $temp | cut -d, -f2)
        sshKillCommand $servername $pid
        log "\tClient is killed at $servername"
    done
    processPids=()
    __count_process=0
}
 
runLatency(){
    local size=$1
    local num=$2
    local filename=$3

    debug "runLatency "
    local comm="$WORKDIR/latency --server=${__corm_server} --num=$num --size=$size"
    log "\tStart runLatency with size=$size"
    ${comm} > $filename
    log "\t runLatency is done"
}
 
runWorkloadRemoteAsync(){
    local server=$1
    local prob=$2
    local num=$3
    local threads=$4
    local seed=$5
    local flags=$6
    local filename=$7
    local comm="$WORKDIR/workload_readwrite --server=${__corm_server} --target=2000000 \
    --prob=${prob} --num=${num} --threads=${threads} --seed=${seed} --input=${WORKDIR}/test.bin  ${flags}"
    echo "$comm"
    local pid=$(sshCommandAsync $server "$comm" $filename)
    log "\tWorkload is started at ${server} with PID$pid"
    processPids[${__count_process}]="$server,$pid"
    __count_process=$((${__count_process}+1))
} 
