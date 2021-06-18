/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * Various read/write workload for CoRM
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
#include "utilities/zipf.hpp"
#include "utilities/ycsb.hpp"

#include "utilities/cxxopts.hpp"
 

using ReadFuncPtr = int (RemoteMemoryClient::*)( LocalObjectHandler* obj,  char* buffer, uint32_t length );

std::atomic<int> order(0);

uint64_t num;

cxxopts::ParseResult
parse(int argc, char* argv[])
{
  cxxopts::Options options(argv[0], "Read write workload for CoRM");
  options
    .positional_help("[optional args]")
    .show_positional_help();

  try
  {

    options.add_options()
      ("server", "Another address", cxxopts::value<std::string>(), "IP")
      ("i,input", "input file", cxxopts::value<std::string>()->default_value("test.bin"), "FILE")
      ("t,threads", "the number of threads", cxxopts::value<uint32_t>()->default_value(std::to_string(1)), "N")
      ("target", "expected rate ops/sec", cxxopts::value<uint64_t>()->default_value(std::to_string(1000)), "N")
      ("p,prob", "Probability of read", cxxopts::value<float>()->default_value(std::to_string(0.5f)), "N")
      ("seed", "seed", cxxopts::value<int>()->default_value(std::to_string(3)), "N")
      ("zipf", "use zipf distribution as in YSCB")
      ("rdmaread", "Use one-sided reads")
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
 


void workload_worker(int threadid, VerbsEP *ep,  ReadFuncPtr readfunc, LocalObjectHandler *objects_orig, uint32_t  NN, bool is_zipf, int seed, float read_prob, uint64_t target){
    RemoteMemoryClient* api = new RemoteMemoryClient(0,ep); 
 
    Trace *trace = nullptr; 
    if (is_zipf)
    {
        trace = new YCSB(seed,read_prob,NN,0.99);
    }
    else
    {
        trace = new Uniform(seed,read_prob,NN);
    }

    LocalObjectHandler *objects =  (LocalObjectHandler*)malloc(NN*sizeof(LocalObjectHandler));
    memcpy((char*)objects,(char*)objects_orig,NN*sizeof(LocalObjectHandler));
 
    uint32_t size = objects[0].requested_size;
    char* buffer = (char*)malloc(size);


    std::chrono::seconds sec(1);
    uint64_t nanodelay =  std::chrono::nanoseconds(sec).count() / target ; // per request
    auto starttime = std::chrono::high_resolution_clock::now();
    
    uint32_t interval = 2560;

    std::vector<uint64_t> request_bw;
    request_bw.reserve(1024);
#ifdef LATENCY
    std::vector<uint64_t> request_latency;
    request_latency.reserve(num);
#endif 
    uint32_t conflicts = 0;
    auto bwt1 = std::chrono::high_resolution_clock::now();
    uint32_t count = 0;
    for(uint64_t i=0; i<num; i++)
    {
        auto req = trace->get_next();
        LocalObjectHandler* obj = &objects[req.first];
        assert(obj!=nullptr && "object cannot be null");


#ifdef LATENCY
        auto t1 = std::chrono::high_resolution_clock::now();
#endif 

        // the following piece of code were used to incur Indirect pointers
//        uint64_t direct_addr = obj->addr.comp.addr;
//	uint64_t base_addr = GetVirtBaseAddr(obj->addr.comp.addr);
//        if(direct_addr == base_addr){
//             base_addr += 32;
//        }
//        obj->addr.comp.addr = base_addr;
        if(req.second == 'r'){

            int ret = (api->*readfunc)(obj, buffer, size);
            if(ret<0){
              conflicts++; 
            }
          //  api->Read(obj, buffer, size);
        } else {
            api->Write(obj, buffer, size, false);
        }
#ifdef LATENCY
        auto t2 = std::chrono::high_resolution_clock::now();
        request_latency.push_back( std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count() );
#endif 
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

 
    while(order.load() != threadid ){

    }

    printf("Data thread #%u: \n",threadid);
    printf("throughput(Kreq/sec): ");
    for(auto &x : request_bw){
        printf("%.2f ",(interval*1000.0)/x);
    }
#ifdef LATENCY
    printf("latency(us): ");
    for(auto &x : request_latency){
        printf("%.2f ",x/1000.0);
    }
#endif 
    printf("\nFinished workload in %lu ms with %u conflicts\n", std::chrono::duration_cast< std::chrono::milliseconds >( endtime  - starttime ).count(), conflicts );
     
    order++;

    return;
}
 
 
int main(int argc, char* argv[]){
 

    auto allparams = parse(argc,argv);
 
    log_fp=stdout;  

    std::string server = allparams["server"].as<std::string>();
    std::string input = allparams["input"].as<std::string>();
    uint64_t target = allparams["target"].as<uint64_t>();
    uint32_t threads = allparams["threads"].as<uint32_t>();
    num = allparams["num"].as<uint64_t>();
    float read_prob = allparams["prob"].as<float>();
    int seed = allparams["seed"].as<int>();

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

    std::fstream fout;
//    printf("File name %s \n",input.c_str());
    fout.open(input.c_str(), std::ios::in|std::ios::binary);
    uint32_t NN = 0;

    fout.read((char*)&NN,sizeof(NN));
 
    LocalObjectHandler *objects;
    objects =  (LocalObjectHandler*)malloc(NN*sizeof(LocalObjectHandler));

    for(uint32_t i = 0; i < NN; i++){
        LocalObjectHandler* obj = &objects[i];
        fout.read((char*)obj,sizeof(LocalObjectHandler));
      //  obj->print();
    }
    fout.close();
    printf("Finished reading %u objects from file\n", NN);

 
    ReadFuncPtr readfunc = nullptr;
    if(allparams.count("rdmaread")){
        readfunc = &RemoteMemoryClient::ReadOneSided;
    }else {
        readfunc = &RemoteMemoryClient::Read;
    }

    std::vector<std::thread> workers;

    for(int i = 0; i < (int)threads; i++){
        workers.push_back(std::thread(workload_worker,i,conns[i],readfunc, objects, NN, allparams.count("zipf"), seed + i, read_prob,target));
    }

    for (auto& th : workers) th.join();
 
    return 0;
}
