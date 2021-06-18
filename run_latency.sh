#!/bin/bash


#                                                                                                      
# CoRM: Compactable Remote Memory over RDMA
# 
# Help functions to measure latency of CoRM
#
# Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#

source core.sh
trap 'echo -ne "Stop all servers..." && killAllProcesses && killCorm && echo "done" && exit 1' INT

define HELP <<'EOF'

Script for measuring latency
usage  : $0 [options]
options: --server=IP   # file containing IP addressof the CoRM server
         --num=INT # the number of records to consume
         --dir=PATH #absolute path to corm 
EOF

usage () {
    echo -e "$HELP"
}


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


startCorm $server "--send_buf_size=65536 --threads=1 --recv_buf_size=4096  --num_recv_buf=256"
 
sleep 0.5

#real:16 user:8;
#real:24 user:15;
#real:32 user:24;
#real:64 user:56;
#real:128 user:118;
#real:248 user:236;
#real:504 user:488;
#real:1016 user:992;
#real:2040 user:2000;
allSizes=(8 15 24 56 118 236 488 992 2000)

echo "Starting latency test"
for size in ${allSizes[@]}; do
  outputfilename=latency_${size}.txt
  runLatency $size $num $outputfilename
done

 
killCorm 
echo "----------Done--------------"
