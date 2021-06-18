all:  remote_read_benchmark workload_readwrite latency server load unload  compaction compact  local_read_benchmark

CFLAGS += -Wall -std=c++14  -O2 -I./ -libverbs  -lpthread -lrdmacm  -lev #-DDEBUG #-g -D_GNU_SOURCE
CPP = #./alloc/alloc_adapter.cpp


remote_read_benchmark: 
	rm -f remote_read_benchmark 
	g++ remote_read_benchmark.cpp  $(CFLAGS) -o remote_read_benchmark

latency: 
	rm -f latency 
	g++ latency.cpp  $(CFLAGS) -o latency

server: 
	rm -f server 
	g++ main.cpp $(CFLAGS) $(CPP) -o server

load: 
	rm -f load 
	g++ load.cpp $(CFLAGS) $(CPP) -o load

unload: 
	rm -f unload 
	g++ unload.cpp $(CFLAGS) $(CPP) -o unload

compaction: 
	rm -f compaction 
	g++ compaction_latency.cpp $(CFLAGS) $(CPP) -o compaction

compact: 
	rm -f compact 
	g++ compact.cpp $(CFLAGS) $(CPP) -o compact

workload_readwrite: 
	rm -f workload_readwrite 
	g++ workload_readwrite.cpp $(CFLAGS) $(CPP) -o workload_readwrite

local_read_benchmark: 
	rm -f local_read_benchmark 
	g++ local_read_benchmark.cpp $(CFLAGS) $(CPP) -o local_read_benchmark

clean:
	rm -f remote_read_benchmark workload_readwrite latency server load unload  compaction compact  local_read_benchmark
.DELETE_ON_ERROR:
.PHONY: all clean
