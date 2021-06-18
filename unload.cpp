/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A simple code to partially unload data from CoRM to have fragmentation.
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
  
#include "utilities/cxxopts.hpp"
 

cxxopts::ParseResult
parse(int argc, char* argv[])
{
  cxxopts::Options options(argv[0], "unload random objects from CoRM");
  options
    .positional_help("[optional args]")
    .show_positional_help();

  try
  {

    options.add_options()
      ("server", "Another address", cxxopts::value<std::string>(), "IP")
      ("i,input", "input file", cxxopts::value<std::string>()->default_value("test.bin"), "FILE")
      ("n,num", "Number of objects to deallocate", cxxopts::value<uint32_t>()->default_value("123"), "N")
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
 
    uint32_t todelete = allparams["num"].as<uint32_t>();
    std::string input = allparams["input"].as<std::string>();
 
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


    if(NN != todelete){
        std::random_shuffle(objects.begin(), objects.end()); 
    }

    for(uint32_t i = 0; i < todelete; i++){
        api->Free(objects[i]);
        free(objects[i]);
    }
    

    if(NN != todelete){
        uint32_t rest = NN-todelete;
        std::fstream fout;
        fout.open(input.c_str(), std::ios::trunc|std::ios::out|std::ios::binary);

        fout.write((char*)&rest,sizeof(rest));

        for(uint32_t i = 0; i < rest; i++){
            fout.write((char*)objects[todelete+i],sizeof(LocalObjectHandler));
        }

        fout.close();
        
        printf("Objects' keys are written to file %s\n", input.c_str());
    } else {
        std::remove(input.c_str()); 
    }
 

    return 0;
}
