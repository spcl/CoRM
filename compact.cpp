/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A simple code to trigger compaction.
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
#include <fstream>
#include <algorithm>
#include <atomic>
 
FILE *log_fp;
 

#include "worker/client_api.hpp"
#include "rdma/connectRDMA.hpp"
 
#include "utilities/cxxopts.hpp"
 
cxxopts::ParseResult
parse(int argc, char* argv[])
{
  cxxopts::Options options(argv[0], "Trigger compaction for a given size");
  options
    .positional_help("[optional args]")
    .show_positional_help();

  try
  {

    options.add_options()
      ("server", "Another address", cxxopts::value<std::string>(), "IP")
      ("size", "objects size", cxxopts::value<uint32_t>()->default_value("24"), "N")
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
 
 
int main(int argc, char* argv[]){
 

    auto allparams = parse(argc,argv);
 
    log_fp=stdout;  

    std::string server = allparams["server"].as<std::string>();
    uint32_t size = allparams["size"].as<uint32_t>();
 
    ClientRDMA rdma((char*)server.c_str(),9999);
    struct rdma_cm_id * id = rdma.sendConnectRequest();

    struct ibv_pd * pd = ClientRDMA::create_pd(id);

    struct ibv_qp_init_attr attr;
    struct rdma_conn_param conn_param;
    memset(&attr, 0, sizeof(attr));
    attr.cap.max_send_wr = 32;
    attr.cap.max_recv_wr = 32;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 0;
    attr.qp_type = IBV_QPT_RC;

    attr.send_cq = ibv_create_cq(pd->context, 32, NULL, NULL, 0);
    attr.recv_cq = ibv_create_cq(pd->context, 32, NULL, NULL, 0);
 
    memset(&conn_param, 0 , sizeof(conn_param));
    conn_param.responder_resources = 0;
    conn_param.initiator_depth = 5;
    conn_param.retry_count = 3;
    conn_param.rnr_retry_count = 3; 

    VerbsEP* ep = ClientRDMA::connectEP(id, &attr, &conn_param, pd);
  
    printf("Connected\n");
    sleep(1);

    RemoteMemoryClient* RMAPI = new RemoteMemoryClient(0,ep);
 
    
    uint8_t slot_type = SizeTable::getInstance().GetClassFromUserSize(size);
    printf("Trigger compaction for size-class %u \n",slot_type);
 
    RMAPI->TriggerCompaction(slot_type, true, true);
    
    sleep(0.5);
    printf("done one\n");
 
    return 0;
}
