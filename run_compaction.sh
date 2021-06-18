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

source core.sh
trap 'echo -ne "Stop all servers..." && killAllProcesses && killCorm && echo "done" && exit 1' INT

define HELP <<'EOF'

Script for starting a compaction experiment.
usage  : $0 [options]
options: --server=IP   # file containing IP addressof the CoRM server
         --num=INT # the number of repetitions
         --dir=PATH #absolute path to CoRM 
EOF

usage () {
    echo -e "$HELP"
}

bits=20 # block size in bits. it is hard-coded in CoRM's code
num=10000
server=""

for arg in "$@"
do
    case ${arg} in
    --help|-help|-h)
        usage
        exit 1
        ;;
    --server=*)
        server=`echo $arg | sed -e 's/--server=//'`
        server=`eval echo ${server}`    # tilde and variable expansion
        ;;
    --num=*)
        num=`echo $arg | sed -e 's/--num=//'`
        num=`eval echo ${num}`    # tilde and variable expansion
        ;;
    --dir=*)
        WORKDIR=`echo $arg | sed -e 's/--dir=//'`
        WORKDIR=`eval echo ${WORKDIR}`    # tilde and variable expansion
        ;;
    esac
done


for ((thread=2;thread<=16;thread=thread*2)); do
    name="collection_${bits}_${thread}_24.txt"
    startCorm $server "--send_buf_size=65536 --threads=${thread} --recv_buf_size=4096  --num_recv_buf=256 --log_file=${WORKDIR}/$name"
    sleep 0.5
    ./compaction --server=$server --threads=${thread} --num=50 --size=24 --collection

    sleep 0.5
    stopCorm 

    sleep 1.5
    killCorm
done

echo "----------Done--------------"


for ((thread=2;thread<=16;thread=thread*2)); do
    name="compaction_${bits}_${thread}_24.txt"
    startCorm $server "--send_buf_size=65536 --threads=${thread} --recv_buf_size=4096  --num_recv_buf=256 --log_file=${WORKDIR}/$name"
    sleep 0.5
    ./compaction --server=$server --threads=${thread} --num=50 --size=24 --collection --compaction

    sleep 0.5
    stopCorm 

    sleep 1.5
    killCorm
done

echo "----------Done--------------"
