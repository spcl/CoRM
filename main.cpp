/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A simple code for launching CoRM
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
 


#include "utilities/cxxopts.hpp"
 


cxxopts::ParseResult
parse(int argc, char* argv[])
{
  cxxopts::Options options(argv[0], "Launch CoRM server");
  options
    .positional_help("[optional args]")
    .show_positional_help();

  try
  {

    options.add_options()
      ("a,server", "server address. I use port 9999", cxxopts::value<std::string>(), "IP")
      ("threads", "Total threads CoRM has", cxxopts::value<uint32_t>()->default_value(std::to_string(1)), "N")
      ("thclass", "threshold class", cxxopts::value<uint32_t>()->default_value(std::to_string(50)), "N")
      ("thpopularity", "threshold popularity", cxxopts::value<uint32_t>()->default_value(std::to_string(100)), "N")
      ("preallocate", "preallocate superblocks", cxxopts::value<uint32_t>()->default_value(std::to_string(1)), "N")
      ("num_recv_buf", "Number of receive buffers per thread", cxxopts::value<uint32_t>()->default_value(std::to_string(256)), "N")
      ("recv_buf_size", "The size of each receive buffer", cxxopts::value<uint32_t>()->default_value(std::to_string(2048)), "N")
      ("send_buf_size", "The total size of send buffer per thread", cxxopts::value<uint32_t>()->default_value(std::to_string(1024*16)), "N")
      ("log_file", "output file", cxxopts::value<std::string>(), "file")
      ("odp", "enable ODP with prefetch if supported")
      ("help", "Print help")
     ;
 
    auto result = options.parse(argc, argv);

    if (result.count("help"))
    {
      std::cout << options.help({""}) << std::endl;
      exit(0);
    }

    if (!result.count("server"))
    {
      throw cxxopts::OptionException("input must be specified");
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
    if(allparams.count("log_file")){
      std::string name = allparams["log_file"].as<std::string>();
      log_fp = fopen(name.c_str(), "w+");
      if (log_fp==NULL) {
        printf("Cannot open log file\n");
        exit(1);
      }
    } else {
      log_fp = stdout;
    }

    std::string ip = allparams["server"].as<std::string>();
    uint32_t threads = allparams["threads"].as<uint32_t>();
 
    uint32_t total_thread_num = threads;
    LauncherMaster *m = new LauncherMaster(total_thread_num);  // 10 threads

    ServerRDMA *server = new ServerRDMA((char *)ip.c_str(), 9999);
    struct ibv_pd *pd = server->create_pd();

    SizeTable::getInstance().PrintTable();

    uint32_t threshold_popular_class = allparams["thpopularity"].as<uint32_t>(); // popular classes are allocated by local thread. The class is popular once it has 100+ requests.
    uint32_t threshold_size_class = allparams["thclass"].as<uint32_t>(); // smaller size classes are allocated by local thread

    AllocAdapter::init(threshold_popular_class, threshold_size_class);

    uint32_t preallocate = allparams["preallocate"].as<uint32_t>(); // how many super blocks preallocate

    BlockAllocImpl *balloc = new BlockAllocImpl(0, preallocate);
    ibv_memory_manager *ibv = new ibv_memory_manager(pd, allparams.count("odp") );

    uint32_t recv_buffers_num = allparams["num_recv_buf"].as<uint32_t>(); 
    uint32_t recv_buffer_size = allparams["recv_buf_size"].as<uint32_t>(); 
    uint32_t send_buffer_size = allparams["send_buf_size"].as<uint32_t>(); 

    for(uint32_t i = 0; i < total_thread_num; i++)
    {
        ThreadAlloc *alloc = new ThreadAllocImpl(i, balloc, ibv);
        AllocAdapter::getInstance().RegThread(alloc, i);
        Worker *w = new Worker(i, alloc, pd, recv_buffers_num, recv_buffer_size, send_buffer_size );
        if(i == 0)
        {
            w->set_rdma_server(server);
        }
        m->add_worker(w);
    }

    m->launch();

    delete m;

    if(allparams.count("log_file")){
      fclose(log_fp);
    }

    return 0;
}
