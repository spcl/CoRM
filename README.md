# CoRM: Compactable Remote Memory over RDMA
A remote memory system that supports compaction over RDMA networks.     
This is the source code for our [SIGMOD 2021 paper](paper/corm.pdf).

## Requirements
 * GCC >= 4.9 with C++14 features
 * rdma-core library, or equivalent RDMA verbs library 
 * RDMA-capable network devices must have assigned IP addresses
 * boost lockfree queue 
 * libev-dev library

## Usage 

To compile the code simply run `make`.    
We provide a series of bash scripts to launch CoRM. For that modify IP address of your servers accordingly.


## Basic usage
```
make
./server -a 192.168.1.10 --threads=1 % to start code with 1 thread. Corm will print size class info and then periodically report stats of the worker thread.
./latency -a 192.168.1.10  % start basic latency test
```

Note the CoRM prints only RPC stats, as it is unaware of completed one-sided RDMA reads.


## Debugging
For debugging include `-DDEBUG` flag in `CFLAGS` of the Makefile. It will enable printing debug messages.

## Implementation details
For research purposes, CoRM has the following implementation artifacts:

#### Connection establishment model
Each new client is directly connected to a remote thread worker.
The thread worker is assigned in a round-robin order. It helps to manage and debug the thread that is responsible for a client. 
To have direct connections to all threads, a client can open multiple connections to CoRM. 

#### Key/Addr sizes
All sizes of blocks, superblocks and ids are hard-coded. Refer to `common/common.hpp` to tune the parameters.

#### Compaction
The compaction is triggered manually. For that you can use `./compact` binary.

#### Loading/Unloading
For loading and unloading data please use `./load` and `./unload` binaries. `load` needs to know the number of remote threads to evenly load each remote worker with objects. It is a side effect of the "Connection establishment model".


## Implementation notice
I am not a professional software developer, and that is why the code is not of the production-level quality.
Notably, I did not invest time in splitting the code into `*.cpp` amd `*.hpp` files to improve compilation process.
Also the settings related to block, ID, and key sizes are hard-coded and can be managed in `common/common.hpp`. 



## Citing this work

If you use our code, please consider citing our [SIGMOD 2021 paper](paper/corm.pdf):

```
@inproceedings{taranov-corm,
author = {Taranov, Konstantin and Di Girolamo, Salvatore and Hoefler, Torsten},
title = {Co{RM}: {C}ompactable {R}emote {M}emory over {RDMA}},
year = {2021},
isbn = {9781450383431},
publisher = {Association for Computing Machinery},
url = {https://doi.org/10.1145/3448016.3452817},
doi = {10.1145/3448016.3452817},
booktitle = {Proceedings of the 2021 ACM SIGMOD International Conference on Management of Data},
location = {Virtual Event, China},
numpages = {14},
series = {SIGMOD'21}
}
```

## Contact 
If you have questions, please, contact:

Konstantin Taranov (konstantin.taranov "at" inf.ethz.ch)    
