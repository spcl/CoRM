/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * Implmenetation of a block allocator. It also manages ownerships of each block.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */

#pragma once

#include "alloc.hpp"


#include "../utilities/block_home_table.h"

#include <unordered_map>
#include <list> 

#include "superblock.hpp"


 
class BlockAllocImpl: public BlockAlloc {


public:

    BlockAllocImpl(uint32_t thread_id, uint32_t prealloc_superblock_num, std::atomic<uint64_t> *bstat = NULL): 
       home_thread_id(thread_id) ,_superblock_counter (0), bstat(bstat)
    {
        preallocate_superblocks(prealloc_superblock_num);

    }
 
    virtual void AllocBlock(ThreadAlloc *alloc, uint8_t type,  block_alloc_cb cb, void *owner) override;
 
    virtual void RemoveVirtAddr(addr_t addr, helper_cb cb, void *owner) override;

    virtual bool FreePhysBlock(_block_phys_addr_t addr, uint8_t type) override;

    virtual uint32_t GetBlockSize() const override;

    virtual ThreadAlloc *GetHomeAlloc(addr_t addr) override; 

    virtual int GetHomeThreadMpIdx() const override{
        return home_thread_id;
    }

    virtual void print_stats() override{
 
        info(log_fp, "[BlockAllocImpl(%d)] Stats; \n", this->home_thread_id );
        // todo. print stats.
    }


 
    virtual void UpdateOwnership( std::forward_list<addr_t> *addresses,   ThreadAlloc *newalloc, install_blocks_cb cb, void *owner) override{
        assert(this->home_thread_id == mts::thread_id && "Only home thread can modify");
        while (!addresses->empty()){
            addr_t addr = addresses->front();
            addresses->pop_front();

            text(log_fp, "[UpdateOwnership] Find %" PRIx64 " \n",addr );
            ThreadAlloc * alloc = home_table.Lookup(addr); 
            assert(alloc!=nullptr && "no addr in table");

            bool changed = home_table.Update(addr, newalloc);
            assert(changed && "Failed to update owner of block addr");
        }
        
        if(cb!=NULL){
            cb(owner);
        }
    }

    // for debugging
    Block* AllocBlock();


    ~BlockAllocImpl(){
         
        for (auto const& item : all_superblocks)
        {
            delete item.second;                   
        }
    }


private:
    const int home_thread_id;
    
    uint32_t _superblock_counter;
 
    BlockHomeTable                          home_table;
    std::atomic<uint64_t>            *const bstat;
    std::unordered_map<int,SuperBlock*>     all_superblocks; 
    std::list<SuperBlock*>                  free_superblocks;
    void preallocate_superblocks(uint32_t prealloc_superblock_num);



};

 

ThreadAlloc*  BlockAllocImpl::GetHomeAlloc(addr_t addr){

    ThreadAlloc * alloc = home_table.Lookup(addr); 
    
    return alloc;
 } 

 
uint32_t BlockAllocImpl::GetBlockSize() const{
    return BLOCK_SIZE;
}


void BlockAllocImpl::RemoveVirtAddr(addr_t addr, helper_cb cb, void *owner){
    home_table.Remove(addr);
    text(log_fp, " RemoveVirtAddr %" PRIx64 " \n",addr); 
    int ret = munmap((void*)addr, BLOCK_SIZE);
    assert(ret==0 && "munmap failed");
    if(cb!=NULL){
        cb(owner);
    }
}

Block* BlockAllocImpl::AllocBlock(){
    SuperBlock *sb;
    if(free_superblocks.empty()){
        sb = new SuperBlock(_superblock_counter++);
        all_superblocks.insert({sb->getFD(), sb});
        free_superblocks.push_front(sb);  
    }
    sb = free_superblocks.front();
    

    Block* b = sb->allocateBlock();
    if(sb->isFull()){
        free_superblocks.pop_front();
    }

    return b;
}

void BlockAllocImpl::AllocBlock(ThreadAlloc *alloc, uint8_t type, block_alloc_cb cb, void *owner){

    Block *b = AllocBlock();
    text(log_fp, "[BlockAllocImpl] insert  %p \n",b);
    addr_t addr = b->CreateNewAddr();
    text(log_fp, "[BlockAllocImpl] insert  %" PRIx64 " \n",addr);
    home_table.Insert(home_thread_id, addr, alloc);
    text(log_fp, "[BlockAllocImpl] insert  %" PRIx64 " \n",addr);
    assert(alloc==home_table.Lookup(addr) && "home_table does not work correctly");
    assert(alloc!=nullptr && "home_table does not work correctly");

    if(bstat!=NULL){
        bstat[type]++;
    }

    cb(b, addr, owner);
}


bool BlockAllocImpl::FreePhysBlock(_block_phys_addr_t phys,  uint8_t type){
    if(bstat!=NULL){
        bstat[type]--;
    }
    auto it = all_superblocks.find(phys.fd);
    assert(it != all_superblocks.end());
    SuperBlock* sb = it->second;
    
    bool wasFull = sb->isFull();

    sb->freeBlock(phys);

    if(wasFull){
        free_superblocks.push_front(sb);  
    }
    
    return true;
};



void BlockAllocImpl::preallocate_superblocks(uint32_t prealloc_superblock_num){
    text(log_fp, "\t\t\t[BlockAllocImpl] preallocate_blocks %" PRIu32 " blocks \n",prealloc_superblock_num );
    for(uint32_t i=0; i< prealloc_superblock_num; i++){
        SuperBlock* sb = new SuperBlock(_superblock_counter++);
        free_superblocks.push_front(sb);
        all_superblocks.insert({sb->getFD(), sb});
    }
}
