/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A simple code to measuring latency of various requests.
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


#include "thread/thread.hpp"
#include "utilities/timer.h"

FILE *log_fp;

#include "worker/client_api.hpp"
#include "rdma/connectRDMA.hpp"

  
#include "utilities/cxxopts.hpp"
 

cxxopts::ParseResult
parse(int argc, char* argv[])
{
  cxxopts::Options options(argv[0], "Various latency test for CoRM");
  options
    .positional_help("[optional args]")
    .show_positional_help();

  try
  {

    options.add_options()
      ("a,server", "Another address", cxxopts::value<std::string>(), "IP")
      ("s,size", "Object size", cxxopts::value<uint32_t>()->default_value("8"), "N")
      ("n,num", "Number of tests", cxxopts::value<uint32_t>()->default_value("1024"), "N")
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
    uint32_t N = allparams["num"].as<uint32_t>();

 
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

    RemoteMemoryClient* RMAPI = new RemoteMemoryClient(0,ep);
    

    std::vector<double> time_alloc;
    std::vector<double> time_free;
    time_alloc.reserve(N);
    time_free.reserve(N);

    printf("Start latency test for allocation/deallocation\n");
    for(uint32_t i = 0; i< N; i++){

        auto t1 = std::chrono::high_resolution_clock::now();
        LocalObjectHandler* obj1 = RMAPI->Alloc(size);
        auto t2 = std::chrono::high_resolution_clock::now();
        RMAPI->Free(obj1);
        auto t3 = std::chrono::high_resolution_clock::now();

        auto alloc_nano = std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count();
        auto free_nano = std::chrono::duration_cast< std::chrono::nanoseconds >( t3 - t2 ).count();
        time_alloc.push_back(alloc_nano / (float)1000.0 );
        time_free.push_back(free_nano / (float)1000.0 );
    }

    printf("Start latency test for read/Write\n");
    std::vector<double> time_read;
    std::vector<double> time_read_rdma;
    std::vector<double> time_write;
    time_read.reserve(N);
    time_read_rdma.reserve(N);
    time_write.reserve(N);

    char* buffer = (char*)malloc(size);
    LocalObjectHandler* obj1 = RMAPI->Alloc(size);
    for(uint32_t i = 0; i< N; i++){
        auto t1 = std::chrono::high_resolution_clock::now();
        RMAPI->Read(obj1, buffer, size);
        auto t2 = std::chrono::high_resolution_clock::now();
        auto read_nano = std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count();
        time_read.push_back(read_nano / (float)1000.0 );
    }

    for(uint32_t i = 0; i< N; i++){
        auto t1 = std::chrono::high_resolution_clock::now();
        RMAPI->ReadOneSided(obj1, buffer, size);
        auto t2 = std::chrono::high_resolution_clock::now();
        auto read_nano = std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count();
        time_read_rdma.push_back(read_nano / (float)1000.0 );
    }

    for(uint32_t i = 0; i< N; i++){
        auto t1 = std::chrono::high_resolution_clock::now();
        RMAPI->Write(obj1, buffer, size, false);
        auto t2 = std::chrono::high_resolution_clock::now();
        auto write_nano = std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count();
        time_write.push_back(write_nano / (float)1000.0 );
    }
    

    printf("Start latency test for fixpointer\n");
    std::vector<double> time_fix;
    std::vector<double> time_fix_read;
    std::vector<double> time_fix_read_rdma;
    std::vector<double> time_fix_write;
    time_fix_write.reserve(N);
    time_fix_read.reserve(N);
    time_fix_read_rdma.reserve(N);
    time_fix.reserve(N);

    LocalObjectHandler* obj2 = RMAPI->Alloc(size);
    uint64_t direct_addr = obj2->addr.comp.addr;
    uint64_t base_addr = GetVirtBaseAddr(obj2->addr.comp.addr);
    if(base_addr == direct_addr){
        printf("Warning! Object was block aligned! the test is not valid for it!\n");
    }
    if(obj2->addr.comp.rkey==0) printf("zero rkey;");
    printf("Start latency test for fix read\n");
    for(uint32_t i = 0; i< N; i++){
        obj2->addr.comp.addr = base_addr; // set wrong offset. so we will need to find the object by ID
        auto t1 = std::chrono::high_resolution_clock::now();
        RMAPI->Read(obj2, buffer, size);
        auto t2 = std::chrono::high_resolution_clock::now();
        auto read_nano = std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count();
        time_fix_read.push_back(read_nano / (float)1000.0 );
    }
    if(obj2->addr.comp.rkey==0) printf("zero rkey;");

    printf("Start latency test for fix read onesided\n");
    for(uint32_t i = 0; i< N; i++){
        obj2->addr.comp.addr = base_addr;
        auto t1 = std::chrono::high_resolution_clock::now();
        int ret = RMAPI->ReadOneSided(obj2, buffer, size);
        if(ret==NOT_FOUND){
            RMAPI->ReadOneSidedFix(obj2, buffer, size);
        }
        auto t2 = std::chrono::high_resolution_clock::now();
        auto read_nano = std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count();
        time_fix_read_rdma.push_back(read_nano / (float)1000.0 );
    }
    printf("Start latency test for fix write\n");
    for(uint32_t i = 0; i< N; i++){
        obj2->addr.comp.addr = base_addr;
        auto t1 = std::chrono::high_resolution_clock::now();
        RMAPI->Write(obj2, buffer, size, false);
        auto t2 = std::chrono::high_resolution_clock::now();
        auto write_nano = std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count();
        time_fix_write.push_back(write_nano / (float)1000.0 );
    }

 printf("Start latency test for fix fix pointer\n");
    for(uint32_t i = 0; i< N; i++){
        obj2->addr.comp.addr = base_addr;
        auto t1 = std::chrono::high_resolution_clock::now();
        RMAPI->FixPointer(obj2);
        auto t2 = std::chrono::high_resolution_clock::now();
        auto fix_nano = std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count();
        time_fix.push_back(fix_nano / (float)1000.0 );
    }



    std::vector<double> time_rpc;
    std::vector<double> time_rdma;
    time_rpc.reserve(N);
    time_rdma.reserve(N);

    printf("Start latency test for RDMA rpc and onesided\n");
    for(uint32_t i = 0; i< N; i++){

        auto t1 = std::chrono::high_resolution_clock::now();
        RMAPI->RpcFake(buffer,size);
        auto t2 = std::chrono::high_resolution_clock::now();
        RMAPI->ReadOneSidedFake(obj1,buffer,size);
        auto t3 = std::chrono::high_resolution_clock::now();

        auto rpc_nano = std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count();
        auto rdma_nano = std::chrono::duration_cast< std::chrono::nanoseconds >( t3 - t2 ).count();
        time_rpc.push_back(rpc_nano / (float)1000.0 );
        time_rdma.push_back(rdma_nano / (float)1000.0 );
    }
    


    printf("Done\n");
    RMAPI->Free(obj1);
    RMAPI->Free(obj2);

    printf("alloc: ");
    for(auto &x : time_alloc){
        printf("%.2f ",x);
    }
    printf("\nfree: ");
    for(auto &x : time_free){
        printf("%.2f ",x);
    }
    printf("\nread: ");
    for(auto &x : time_read){
        printf("%.2f ",x);
    }
    printf("\nreadrdma: ");
    for(auto &x : time_read_rdma){
        printf("%.2f ",x);
    }
    printf("\nwrite: ");
    for(auto &x : time_write){
        printf("%.2f ",x);
    }
    printf("\nfixread: ");
    for(auto &x : time_fix_read){
        printf("%.2f ",x);
    }
    printf("\nfixreadrdma: ");
    for(auto &x : time_fix_read_rdma){
        printf("%.2f ",x);
    }
    printf("\nfixwrite: ");
    for(auto &x : time_fix_write){
        printf("%.2f ",x);
    }
    printf("\nfixfix: ");
    for(auto &x : time_fix){
        printf("%.2f ",x);
    }
    printf("\nrpc: ");
    for(auto &x : time_rpc){
        printf("%.2f ",x);
    }
    printf("\nrdma: ");
    for(auto &x : time_rdma){
        printf("%.2f ",x);
    }
    printf("\n");
    free(buffer);
    return 0;
}
