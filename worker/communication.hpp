/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * Types for server-client communication
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */


#pragma once 

 
#include "../common/common.hpp"

struct request_t{
    uint8_t type;
    uint8_t version;
    uint32_t size;
    uint32_t req_id;
    client_addr_t addr;
};

enum RequestType: uint8_t
{
    READ = 1,
    WRITE,
    WRITEATOMIC,
    ALLOC,
    FREE,
    FIXPOINTER,
    COMPACT, // for debugging and benchmarking
    DISCONNECT
};

struct message_header_t{
    uint8_t thread_id;   // destination/source lid
    uint8_t type;        // type of the message
};

struct reply_t{
    uint8_t version;
    uint8_t status;
    client_addr_t ret_addr;
    uint32_t data_size;
    uint32_t id;
};

 