/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A simple code to measure Remote Read throughput for Farm/Mesh/Corm
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
 

#include "thread/thread.hpp" 
FILE *log_fp;



#include "worker/client_api.hpp"
#include "rdma/connectRDMA.hpp"
#include "utilities/zipf.hpp"
#include "utilities/ycsb.hpp"

#include "utilities/cxxopts.hpp"
 

cxxopts::ParseResult
parse(int argc, char* argv[])
{
  cxxopts::Options options(argv[0], "Remote Read benchmark for Farm/Mesh/Corm");
  options
    .positional_help("[optional args]")
    .show_positional_help();

  try
  {

    options.add_options()
      ("server", "Another address", cxxopts::value<std::string>(), "IP")
      ("i,input", "input file", cxxopts::value<std::string>()->default_value("test.bin"), "FILE")
      ("target", "expected rate ops/sec", cxxopts::value<uint64_t>()->default_value(std::to_string(1000)), "N")
      ("rpc", "Use rpc reads")
      ("rdmaread", "Use one-sided reads")
      ("mesh", "Use mesh reads")
      ("farm", "Use farm reads")
      ("n,num", "Number of requests to run", cxxopts::value<uint64_t>()->default_value("123"), "N")
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
 
    int seed = 3;

    auto allparams = parse(argc,argv);
 
    log_fp=stdout;  

    std::string server = allparams["server"].as<std::string>();
    std::string input = allparams["input"].as<std::string>();
    uint64_t target = allparams["target"].as<uint64_t>();
    uint64_t num = allparams["num"].as<uint64_t>();
 
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

    memset(&conn_param, 0 , sizeof(conn_param));
    conn_param.responder_resources = 0;
    conn_param.initiator_depth = 5;
    conn_param.retry_count = 3;
    conn_param.rnr_retry_count = 3; 
    
    VerbsEP* ep = ClientRDMA::connectEP(id, &attr, &conn_param, pd);
 
    printf("Connected\n");
    sleep(1);

    RemoteMemoryClient* api = new RemoteMemoryClient(0,ep); 
    
    std::fstream fout;
    fout.open(input.c_str(), std::ios::in|std::ios::binary);
    uint32_t NN = 0;

    fout.read((char*)&NN,sizeof(NN));
 
    std::vector<LocalObjectHandler*> objects(NN);

    for(uint32_t i = 0; i < NN; i++){
        LocalObjectHandler* obj = (LocalObjectHandler*)malloc(sizeof(LocalObjectHandler));
        fout.read((char*)obj,sizeof(LocalObjectHandler));
      //  obj->print();
        objects[i] = obj;
    }
    fout.close();
    printf("Finished reading %u objects from file\n", NN);
    uint32_t size = objects[0]->requested_size;
    char* buffer = (char*)malloc(size);

    Trace *trace = new Uniform(seed,1.0,NN);
 
    using ReadFuncPtr = int (RemoteMemoryClient::*)( LocalObjectHandler* obj,  char* buffer, uint32_t length );

    ReadFuncPtr readfunc = nullptr;

    if(allparams.count("rdmaread")){
        if(allparams.count("mesh")){
            readfunc = &RemoteMemoryClient::ReadOneSidedFake;
        }else  if(allparams.count("farm")){ 
            readfunc = &RemoteMemoryClient::ReadOneSidedFarm;
        }
        else{
            readfunc = &RemoteMemoryClient::ReadOneSided;
        }
    } else {
        if(allparams.count("mesh")){
            readfunc = &RemoteMemoryClient::RpcFakeMesh;
        }else  if(allparams.count("farm")){ 
            readfunc = &RemoteMemoryClient::Read;
        }
        else{
            readfunc = &RemoteMemoryClient::Read;
        }
    }

    std::chrono::seconds sec(1);

    uint64_t nanodelay =  std::chrono::nanoseconds(sec).count() / target ; // per request
    auto starttime = std::chrono::high_resolution_clock::now();
    
    uint32_t interval = 256;

    std::vector<uint64_t> request_bw;
    request_bw.reserve(1024);

    auto bwt1 = std::chrono::high_resolution_clock::now();
    uint32_t count = 0;
    auto req = trace->get_next();
    for(uint64_t i=0; i<num; i++)
    {
        
        assert(req.first<objects.size() && "generated number is out of bound");
        LocalObjectHandler* obj = objects[req.first];
        assert(obj!=nullptr && "object cannot be null");

        int ret = (api->*readfunc)(obj, buffer, size);
        assert(ret==0 && "one sided read failed");

        count++;
        if(count > interval){
            auto bwt2 = std::chrono::high_resolution_clock::now();
            request_bw.push_back(std::chrono::duration_cast<std::chrono::microseconds>(bwt2 - bwt1).count());
            bwt1 = bwt2; 
            count=0;  
        }

        auto const sleep_end_time =  starttime + std::chrono::nanoseconds(nanodelay*i);
        while (std::chrono::high_resolution_clock::now() < sleep_end_time){
            // nothing
        }
    }
    auto endtime = std::chrono::high_resolution_clock::now();

    printf("throughput(Kreq/sec): ");
    for(auto &x : request_bw){
        printf("%.2f ",(interval*1000.0)/x);
    }
 
    printf("\nFinished workload in %lu ms\n", std::chrono::duration_cast< std::chrono::milliseconds >( endtime  - starttime ).count() );
 
    return 0;
}
