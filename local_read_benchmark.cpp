/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A simple code for measuring read local bandwidth
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */
#include <iostream>       // std::cout
#include <thread>
#include <chrono>
#include <vector>
#include <cstdlib>
#include <string>
#include <sstream>
#include <cassert>
#include <stdlib.h>
#include <csignal>
#include <set>

FILE *log_fp;

#include "alloc/block_alloc.hpp"
#include <random>

#include <alloc/size_table.hpp>
#include "alloc/thread_alloc.hpp"
#include "worker/worker.hpp"
#include "rdma/rdma_memory_manager.hpp"
 
#include "utilities/zipf.hpp"
#include "utilities/ycsb.hpp"

#include "utilities/cxxopts.hpp"
 


cxxopts::ParseResult
parse(int argc, char* argv[])
{
  cxxopts::Options options(argv[0], "simple read local bandwidth benchmark");
  options
    .positional_help("[optional args]")
    .show_positional_help();

  try
  {

    options.add_options()
      ("size", "user entry size", cxxopts::value<uint32_t>()->default_value(std::to_string(8)), "N")
      ("help", "Print help")
     ;
 
    auto result = options.parse(argc, argv);

    if (result.count("help"))
    {
      std::cout << options.help({""}) << std::endl;
      exit(0);
    }

    return result;

  } catch (const cxxopts::OptionException& e)
  {
    std::cout << "error parsing options: " << e.what() << std::endl;
    std::cout << options.help({""}) << std::endl;
    exit(1);
  }
}


int main(int argc, char* argv[])
{

    auto allparams = parse(argc,argv);

    log_fp = stdout;
    uint32_t user_size = allparams["size"].as<uint32_t>();

    size_t totest = 1024*1024*1024*1ULL; // 1 GiB
    size_t sbsize = BLOCKS_IN_SUPERBLOCK * BLOCK_SIZE; 
     
	uint8_t type = SizeTable::getInstance().GetClassFromUserSize(user_size);
	uint16_t slot_size = SizeTable::getInstance().GetRealSize(type); 

	uint32_t slotsperblock = BLOCK_SIZE / slot_size;

	printf("entry size %u\n",type);

	std::vector<SuperBlock*> superblocks;

	for(size_t i=1; i <= totest/sbsize; i++){
		superblocks.push_back(new SuperBlock(i));
	}

	printf("total superblocks %lu\n",superblocks.size());

	std::vector<Block*> blocks;
	std::vector<uint64_t> addresses;

	uint8_t version = 0;
	uint16_t object_id = 1;

	for( auto & sb : superblocks){
		char* src = (char*)malloc(user_size);
		while(sb->hasFreeBlocks()){
			Block *b = sb->allocateBlock();
			uint64_t alligned_addr = b->CreateNewAddr();
			blocks.push_back(b);
			addresses.push_back(alligned_addr);

			ReaderWriter::WriteBlockHeader(alligned_addr, type , 0);
			for(uint32_t i =0; i< slotsperblock; i++){
	
				ReaderWriter::SetNewObject(alligned_addr + i*slot_size, object_id, &version );

				ReaderWriter::WriteBufToObject(alligned_addr + i*slot_size,  object_id, slot_size, 
                                  src, user_size, NULL, NULL);
			}
		}
		free(src);
	}
	printf("total blocks %lu\n",blocks.size());
	uint64_t totalobj = blocks.size()*slotsperblock;
	printf("total objects %lu\n",totalobj);

 	Trace *trace = new Uniform(10,0.0,(uint32_t)totalobj);

	char* dest = (char*)malloc(user_size);

	std::vector<uint64_t> accesses;

	uint32_t maxi = totest*4/slot_size;

	for(uint32_t i  = 0; i< maxi; i++){
			uint32_t id = trace->get_next().first;
			uint64_t alligned_addr = addresses[id/slotsperblock] + (id%slotsperblock)*slot_size;
			accesses.push_back(alligned_addr);
	}
	 
 

	{
		auto t1 = std::chrono::high_resolution_clock::now();
		for(uint32_t i  = 0; i< maxi; i++){
			uint64_t alligned_addr = accesses[i];
			uint32_t lim_size = user_size;
			ReaderWriter::client_read_object_to_buffer_lim((uint64_t)dest, (uint64_t)alligned_addr, 
		    (uint64_t)alligned_addr, slot_size,object_id, &lim_size);
		}
		auto t2 = std::chrono::high_resolution_clock::now();
		printf("CoRM Time: %lu\n", std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count() );
	}

	{
		auto t1 = std::chrono::high_resolution_clock::now();
		for(uint32_t i  = 0; i< maxi; i++){
			uint64_t alligned_addr = accesses[i];
			uint32_t lim_size = user_size;
			ReaderWriter::client_read_object_farm((uint64_t)dest, (uint64_t)alligned_addr, 
		    (uint64_t)alligned_addr, slot_size, &lim_size);
		}
		auto t2 = std::chrono::high_resolution_clock::now();
		printf("Farm Time: %lu\n", std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count() );
	}

	{
		auto t1 = std::chrono::high_resolution_clock::now();
		for(uint32_t i  = 0; i< maxi; i++){
			uint64_t alligned_addr = accesses[i];
			uint32_t lim_size = user_size;
			ReaderWriter::client_read_fast((uint64_t)dest, (uint64_t)alligned_addr, slot_size, &lim_size);
		}
		auto t2 = std::chrono::high_resolution_clock::now();
		printf("Mesh Time: %lu\n", std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count() );
	}

    return 0;
}
