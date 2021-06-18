#!/bin/bash


#                                                                                                      
# CoRM: Compactable Remote Memory over RDMA
# 
# Help functions to deploy CoRM and run read/write benchmarks
#
# Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#

source core.sh
trap 'echo -ne "Stop all servers..." && killAllProcesses && killCorm && echo "done" && exit 1' INT

define HELP <<'EOF'

Script for running read/write benchmarks
The client is always launched locally.
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


startCorm $server "--send_buf_size=116384 --threads=8 --recv_buf_size=4096  --num_recv_buf=256"
 
sleep 0.5

loadCorm $size $num


allReadProb=(0.5) #1.0
allFlags=("" "--zipf" "--rdmaread" "--zipf --rdmaread")
allThreads=() #1 2 4

#allReadProb=(1.0)
#declare -a allFlags=("" "--zipf")
#allThreads=(1)

echo "Starting throughput  test"
for th in ${allThreads[@]}; do
    continue
    for rp in ${allReadProb[@]}; do
        for flag in "${allFlags[@]}"; do
            suffix=${flag//[[:blank:]]/}
            outputfilename=workload_1_1_${th}_${rp}_${suffix}.txt
            ./workload_readwrite --server=${__corm_server} --target=2000000 --prob=${rp} --num=1000000 --threads=$th --seed=10 ${flag}> $outputfilename
            echo "--------------done  $outputfilename"
        done
    done
done

nodes=("192.168.1.72" "192.168.1.73" "192.168.1.74" "192.168.1.75"  "192.168.1.76")
echo "Starting throughput  test woth multiple nodes"
echo "The clients will be deployed at ${nodes[@]}"
 
for rp in ${allReadProb[@]}; do
    for flag in "${allFlags[@]}"; do
        for ((total=2;total<=4;++total)); do
            seed=111
            for ((client=1;client<total;++client)); do
                ip=${nodes[$client]}
                suffix=${flag//[[:blank:]]/}
                outputfilename=workload_${client}_${total}_${th}_${rp}_${suffix}.txt
                runWorkloadRemoteAsync $ip $rp 800000 8 $seed "$flag" $WORKDIR/$outputfilename
                seed=$(($seed + 20))
            done
            client=$total
            outputfilename=workload_${client}_${total}_8_${rp}_${suffix}.txt
            ./workload_readwrite --server=${__corm_server} --target=2000000 --prob=${rp} --num=1000000 --threads=8 --seed=$seed ${flag}> $outputfilename
            sleep 4
            echo "------------------done $outputfilename"
            killAllProcesses # for debugging
#            break #for debugging
        done
 #       break
    done
  #  break
done



 

killCorm 
echo "----------Done--------------"
