/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A simple code to measure compaction latency.
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
 
 

uint64_t num;

cxxopts::ParseResult
parse(int argc, char* argv[])
{
  cxxopts::Options options(argv[0], "Measure compaction latency");
  options
    .positional_help("[optional args]")
    .show_positional_help();

  try
  {

    options.add_options()
      ("server", "Another address", cxxopts::value<std::string>(), "IP")
      ("t,threads", "the number of remote threads", cxxopts::value<uint32_t>()->default_value(std::to_string(1)), "N")
      ("n,num", "Number of requests to run", cxxopts::value<uint64_t>()->default_value("123"), "N")
      ("size", "objects size", cxxopts::value<uint32_t>()->default_value("24"), "N")
      ("compaction", "enable compact objects")
      ("collection", "enable collection objects")
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
    uint32_t threads = allparams["threads"].as<uint32_t>();
    uint32_t size = allparams["size"].as<uint32_t>();
    num = allparams["num"].as<uint64_t>();

    bool with_compaction = allparams.count("compaction");
    bool with_collection = allparams.count("collection");
 
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

    std::vector<VerbsEP*> conns; 

    conns.push_back(ClientRDMA::connectEP(id, &attr, &conn_param, pd));

    for(uint32_t i = 1 ; i < threads; i++){
        struct rdma_cm_id * tid = rdma.sendConnectRequest();
        attr.send_cq = ibv_create_cq(pd->context, 32, NULL, NULL, 0);
        attr.recv_cq = ibv_create_cq(pd->context, 32, NULL, NULL, 0);
        conns.push_back(ClientRDMA::connectEP(tid, &attr, &conn_param, pd));
    }
    
    if(threads>1){
        assert(conns[0]->qp->send_cq != conns[1]->qp->send_cq && "Different connections must use Different CQ") ;
    }
 
    printf("Connected\n");
    sleep(1);

    std::vector<RemoteMemoryClient*> RMAPI;
    for( auto ep:conns ){
        RMAPI.push_back(new RemoteMemoryClient(0,ep));
    }
  

    printf("Start compaction test \n");
    for(uint32_t i = 0; i< num; i++){
        std::vector<LocalObjectHandler*> objects;
        
        for( auto x : RMAPI ){
            LocalObjectHandler* obj1 = x->Alloc(size);
            objects.push_back(obj1);
        }
        uint8_t slot_type = objects[0]->addr.comp.type;
        RMAPI[0]->TriggerCompaction(slot_type,with_collection, with_compaction);
        sleep(1.2);
        for(uint32_t i = 0; i < threads; i++){
            RMAPI[i]->Free(objects[i]);
        }
        sleep(0.5);
        printf("done one\n");
    }

    printf("Done compaction test\n");
 
    return 0;
}
