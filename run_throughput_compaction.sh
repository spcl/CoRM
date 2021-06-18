#!/bin/bash


#                                                                                                      
# CoRM: Compactable Remote Memory over RDMA
# 
# Help functions to deploy CoRM and do a compaction experiments
#
# Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#

source core.sh
trap 'echo -ne "Stop all servers..." && killAllProcesses && killCorm && echo "done" && exit 1' INT

define HELP <<'EOF'

Script for measuring compaction latency
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

 

allReadProb=(0.5 1.0 0.95) #1.0
allFlags=("" "--zipf" "--rdmaread" "--zipf --rdmaread")
 
 
nodes=("192.168.1.72" "192.168.1.73" "192.168.1.74" "192.168.1.75"  "192.168.1.76")
echo "Starting throughput  test woth multiple nodes"
echo "The clients will be deployed at ${nodes[@]}"
 
for rp in ${allReadProb[@]}; do
    for flag in "${allFlags[@]}"; do
        for ((total=1;total<=4;++total)); do
            seed=111
            startCorm $server "--send_buf_size=116384 --threads=8 --recv_buf_size=4096  --num_recv_buf=256"
            sleep 0.5
            loadCorm $size $num
            echo "loading is done"
            sleep 0.5
            unloadCorm "$(echo $num*0.8/1 | bc)"
            echo "unloading is done"
            sleep 0.5
            for ((client=0;client<total;++client)); do
                ip=${nodes[$client]}
                suffix=${flag//[[:blank:]]/}
                outputfilename=compaction_${client}_${total}_8_${rp}_${suffix}.txt
                runWorkloadRemoteAsync $ip $rp 800000 8 $seed "$flag" $WORKDIR/$outputfilename
                seed=$(($seed + 20))
            done
            sleep 0.2
            ./compact --server=$server --size=$size # trigger compaction
  
            sleep 20
            echo "------------------done $outputfilename"
            killAllProcesses # for debugging
            stopCorm
            sleep 1
            killCorm
         #   break #for debugging
        done
    #   break
    done
  #  break
done


  
echo "----------Done--------------"
