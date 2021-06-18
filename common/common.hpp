/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * It is a file responsible for corm's settings. It helps to choose size of pointers, addr_t, block headers.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */


#pragma once

#include "../utilities/debug.h"
#include <string.h>
#include <atomic>
#include <type_traits>
 
typedef unsigned __int128 uint128_t;


static const uint64_t CACHELINE = 64ULL;
static const uint64_t CACHELINE_MASK = ~((uint64_t)0x3F);

static constexpr uint64_t mask_of_bits(uint16_t bits){
    return ((uint64_t)1 << bits) - 1;
}

static_assert ( mask_of_bits(16) == 0xFFFF , "mask_of_bits does not work correctly") ;
static_assert ( mask_of_bits(2) == 0b11 , "mask_of_bits does not work correctly") ;
static_assert ( mask_of_bits(12) == 0xFFF , "mask_of_bits does not work correctly") ;

// Note that in my experiments I manually changed the sizes and recompiled the code. 

// 4KB blocks
//static const size_t PAGE_SIZE = (4096); 
 
static const uint32_t BLOCK_BIT_SIZE = (12) ;
static const uint32_t BLOCK_SIZE = (uint32_t)(1 << BLOCK_BIT_SIZE); 

#define BLOCKS_IN_SUPERBLOCK (16)

/*
static const uint32_t BLOCK_BIT_SIZE = (30) ;
static const uint32_t BLOCK_SIZE = (uint32_t)(1 << BLOCK_BIT_SIZE); 

#define BLOCKS_IN_SUPERBLOCK (1)
*/

// client pointer:   | object addr  | object id | rkey    | type    | padding |
// 128bits:          |  48 bits     | 16 bits   | 32 bits |  8 bits |   24    |

#define VIRTUAL_ADDRESS_SIZE (48) // on linux x86-64 it is guaranteed
#define ID_SIZE_BITS   (16) // 2^16 different ids
#define TYPE_SIZE (8) 
#define BASE_ADDR_BITS (VIRTUAL_ADDRESS_SIZE - BLOCK_BIT_SIZE) // 36


typedef union {
    uint128_t whole;
    struct {
      //  uint64_t padding: 128 - TYPE_SIZE - 32 - ID_SIZE_BITS - VIRTUAL_ADDRESS_SIZE - 8;
        uint64_t version: 8;
        uint64_t type   : TYPE_SIZE;
        uint64_t obj_id : ID_SIZE_BITS;
        uint64_t rkey   : 32;
        uint64_t addr ;//  : VIRTUAL_ADDRESS_SIZE;
    } comp;
    uint64_t parts[2];
} client_addr_t;  

typedef uint64_t addr_t;  // actual memory address

typedef uint32_t offset_t;  // offset from base 
 

#define ADDR_T_TO_PTR(x) ((void*)x)


typedef struct {
    int fd;
    uint32_t offset_in_blocks;
} _block_phys_addr_t;

 
typedef union {
    uint128_t whole;
    struct {
        uint64_t padding : 32 - TYPE_SIZE;
        uint64_t type    : TYPE_SIZE;
        uint64_t rkey    : 32;
        uint64_t base    : 64; //BASE_ADDR_BITS
    } comp;
    uint64_t parts[2];
} block_header_t;

static const uint32_t BLOCK_USEFUL_SIZE = (BLOCK_SIZE - sizeof(block_header_t));

// Slot header: | V obj | Lock  | compaction | old addr | id     |
//              |8 bits | 1 bit | 1   bits  | BASE_ADDR_BITS bits  | ID_SIZE_BITS bits | = 16 bytes per object

struct slot_header_t{
    uint64_t version : 8;
    uint64_t lock    : 1;  
    uint64_t allocated : 1;
    uint64_t compaction : 1;  // the idea is that if slot is compacted it means the object most likely has been moved
    uint64_t padding: (53 - ID_SIZE_BITS - BASE_ADDR_BITS);
    uint64_t obj_id : ID_SIZE_BITS;
    uint64_t oldbase : BASE_ADDR_BITS; // is used to distinguish between native objects in block and one which came from another block
};



static_assert(std::is_pod<slot_header_t>::value, "slot_header_t is not POD");
static_assert(std::is_pod<client_addr_t>::value, "client_addr_t is not POD");
static_assert(std::is_pod<block_header_t>::value, "block_header_t is not POD");
static_assert(sizeof(client_addr_t) == sizeof(uint128_t), "client_addr_t is not 128 bits?");
static_assert(sizeof(slot_header_t) == sizeof(uint64_t), "slot_header_t is not 64 bits?");
static_assert(sizeof(uint64_t) == sizeof(unsigned long), " Type check ");
static_assert(sizeof(uint64_t) == sizeof(unsigned long long), " Type check ");


inline addr_t GetVirtBaseAddr(addr_t addr){
    return (addr >> BLOCK_BIT_SIZE) << BLOCK_BIT_SIZE ;
}

inline addr_t GetAddressOffset(addr_t addr){
    return addr & mask_of_bits(BLOCK_BIT_SIZE);
}

inline uint8_t GetSlotType(addr_t addr){
    addr_t baseaddr = GetVirtBaseAddr(addr);
    return ((block_header_t*)(baseaddr+BLOCK_USEFUL_SIZE))->comp.type;
}

inline addr_t GetSlotNewestBaseAddr(addr_t addr){
    addr_t baseaddr = GetVirtBaseAddr(addr);
    return (addr_t)(((block_header_t*)(baseaddr+BLOCK_USEFUL_SIZE))->comp.base);
    //return (addr_t)(((addr_t)((block_header_t*)(baseaddr+BLOCK_USEFUL_SIZE))->comp.base) << BLOCK_BIT_SIZE);
}

inline uint32_t GetSlotNewestRkey(addr_t addr){
    addr_t baseaddr = GetVirtBaseAddr(addr);
    return (uint32_t)(((block_header_t*)(baseaddr+BLOCK_USEFUL_SIZE))->comp.rkey);
}

inline uint8_t GetSlotVersion(addr_t addr){
    return (uint8_t)(((slot_header_t*)(char*)addr)->version);
}

inline uint16_t GetSlotObjId(addr_t addr){
    return (uint16_t)(((slot_header_t*)(char*)addr)->obj_id);
}

inline client_addr_t CreateClientAddr(addr_t addr, uint32_t rkey, uint16_t obj_id, uint8_t version, uint8_t type){
    client_addr_t client_addr;
    client_addr.comp.version  = version;
    client_addr.comp.type  = type;
    client_addr.comp.rkey = rkey;
    client_addr.comp.obj_id = obj_id;
    client_addr.comp.addr = addr;
    return client_addr;
} 


inline addr_t GetObjectAddr(client_addr_t addr){
    return (addr_t)(addr.comp.addr);
}

inline uint16_t GetObjId(client_addr_t client_addr){
    return (uint16_t)client_addr.comp.obj_id;  
}


