#!/bin/bash

#                                                                                                      
# CoRM: Compactable Remote Memory over RDMA
# 
# Help functions to deploy CoRM and read thoughput tests
#
# Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#

source core.sh
trap 'echo -ne "Stop all servers..." && killAllProcesses && killCorm && echo "done" && exit 1' INT

define HELP <<'EOF'

Script for starting a single consumer latency benchmark.
The consumer is always launched locally.
usage  : $0 [options]
options: --server=IP   # file containing IP addressof the CoRM server
         --num=INT # the number of records to consume
         --dir=PATH #absolute path to corm 
EOF

usage () {
    echo -e "$HELP"
}


num=8000000
size=24
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
    --size=*)
        size=`echo $arg | sed -e 's/--size=//'`
        size=`eval echo ${size}`    # tilde and variable expansion
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


startCorm $server "--send_buf_size=116384 --threads=1 --recv_buf_size=4096  --num_recv_buf=256"
 
sleep 0.5

loadCorm $size $num

allFlags=("--rpc" "--rpc --farm" "--rpc --mesh" "--rdmaread" "--rdmaread --farm" "--rdmaread --mesh")


echo "Starting throughput  test"

for flag in "${allFlags[@]}"; do
    suffix=${flag//[[:blank:]]/}
    outputfilename=meshworkload_${size}_${suffix}.txt
    echo ${outputfilename}
    ./remote_read_benchmark --server=${__corm_server} --target=2000000 --num=1000000 --seed=10 ${flag}> $outputfilename
    echo "--------------done  $outputfilename"
done


killCorm 
echo "----------Done--------------"
