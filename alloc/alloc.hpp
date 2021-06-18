/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * Interfaces and callbacks for allocators
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */

#pragma once

#include "block.hpp"
#include "../common/common.hpp"
#include <chrono>
#include <boost/lockfree/queue.hpp>
 
static const int SUCCESS = 0;
static const int BLOCK_IS_NOT_FOUND = -1;
static const int OBJECT_DOES_NOT_EXIST = -2;
 

// Thread allocator callbacks.
typedef void (*thread_alloc_cb)( client_addr_t ret_addr, void *owner);
typedef void (*thread_free_cb)( int status,  addr_t newaddr, void *owner);
typedef void (*thread_find_cb)( addr_t newaddr,  uint16_t slot_size,  void *owner);
typedef void (*fixpointer_cb)( int ret, client_addr_t ret_addr, void *owner);
typedef void (*helper_cb)( void *owner);
 

// A per-thread allocator object.
// this class should be responsible for assigning object ids
class ThreadAlloc {
public:

    struct CompactionCtx{
        boost::lockfree::queue<void*> q;
        std::atomic<uint32_t> counter;
        const uint8_t type;
        const uint8_t initiator;
        ThreadAlloc* master;
        const bool with_compaction;
        std::chrono::time_point<std::chrono::high_resolution_clock> t1;
 
        CompactionCtx(uint8_t num_threads, uint8_t type, uint8_t initiator, ThreadAlloc* master, bool with_compaction):
        q(num_threads), counter(num_threads-1), type(type), initiator(initiator), master(master), with_compaction(with_compaction)
        {
            t1 = std::chrono::high_resolution_clock::now(); 
        }
    };


    virtual ~ThreadAlloc() = default;

    // Allocate a slot.
    virtual void Alloc(uint32_t size, thread_alloc_cb cb, void *owner) = 0;


    // Allocate a slot via adapter.
    virtual void AllocAtHome(ThreadAlloc* alloc, uint32_t size, thread_alloc_cb cb, void *owner) = 0;

    // Free a previously allocated block on this server.
    virtual void Free(client_addr_t client_addr, thread_free_cb cb, void *owner) = 0;

    // shortcut to find address of objects
    virtual void FindObjectAddr(client_addr_t client_addr, thread_find_cb cb, void *owner) = 0;

    // Get the id of the home thread.
    virtual int GetHomeThreadMpIdx() const = 0;

    virtual void FixClientAddr(client_addr_t client_addr, fixpointer_cb cb, void *owner ) = 0;

    virtual void Compaction(uint8_t type) = 0;

    virtual void print_stats() = 0;

    virtual void SendBlocksTo(CompactionCtx *ctx, helper_cb cb, void* owner) = 0;
};


#include <forward_list>
typedef void (*block_alloc_cb)(Block *b, addr_t addr, void *owner);
typedef void (*block_free_cb)(bool success, void *owner);
typedef void (*install_blocks_cb)( void *owner);

class BlockAlloc {
public:
    virtual ~BlockAlloc() = default;
   
    virtual void AllocBlock(ThreadAlloc *alloc, uint8_t type, block_alloc_cb cb, void *owner) = 0;

    virtual void RemoveVirtAddr(addr_t addr, helper_cb cb, void *owner) = 0;

    virtual bool FreePhysBlock(_block_phys_addr_t addr, uint8_t type) = 0;

    virtual uint32_t GetBlockSize() const = 0;
    // get the thread that owns this address
    virtual ThreadAlloc *GetHomeAlloc(addr_t addr) = 0 ; 
 
    virtual int GetHomeThreadMpIdx() const = 0 ;

    virtual void print_stats() = 0;
    // move blocks from one thread to another
    virtual void UpdateOwnership( std::forward_list<addr_t> *addresses, ThreadAlloc *newalloc, install_blocks_cb cb, void *owner) = 0;
};

