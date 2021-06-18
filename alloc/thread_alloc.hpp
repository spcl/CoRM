/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * It is a thread-local allocator. It manages all resources of a thread worker.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */


#pragma once
#include "alloc.hpp"
#include <unordered_map>
#include <forward_list>
#include <list>
#include <map>
#include <random>
#include <algorithm>
#include <infiniband/verbs.h>
#include <set>
#include <chrono>
#include <vector>

#include "size_table.hpp"
#include "../worker/ReaderWriter.hpp"

#include "../thread/messenger.hpp"

#include "local_block.hpp"

#include "../rdma/rdma_memory_manager.hpp"

class ThreadAllocImpl: public ThreadAlloc
{




public:

    ThreadAllocImpl(int  thread_id, BlockAlloc *sballoc, ibv_memory_manager *ibv_rc);

    // Allocate a block.
    void Alloc(uint32_t size, thread_alloc_cb cb, void *owner)  override;
    void AllocAtHome(ThreadAlloc *alloc, uint32_t size, thread_alloc_cb cb, void *owner) override;

    static void AllocTMsg(mts::thread_msg_t *msg);
    static void AllocMsgCb( client_addr_t ret_addr, void *owner);
    static void AllocMsgRetCb(mts::thread_msg_t *msg);


    // Free a previously allocated block on this server.
    void Free(client_addr_t client_addr,  thread_free_cb cb, void *owner) override;

    void FindObjectAddr(client_addr_t client_addr, thread_find_cb cb, void *owner) override;
    void FindObjectLocal(client_addr_t client_addr, thread_find_cb cb, void *owner);

    static void FindObjectTMsg(mts::thread_msg_t *msg);
    static void FindObjectMsgCb(addr_t newaddr,  uint16_t slot_size,  void *owner);
    static void FindObjectRetCb(mts::thread_msg_t *msg);

    // Get the id of the home thread.
    int GetHomeThreadMpIdx() const override
    {
        return home_thread_id;
    }

    void FixClientAddr(client_addr_t client_addr, fixpointer_cb cb, void *owner ) override;

    void Compaction(uint8_t type) override;

    virtual void print_stats() override
    {
        if(sballoc->GetHomeThreadMpIdx() == this->home_thread_id)
        {
            sballoc->print_stats();
        }
        info(log_fp, "[ThreadAllocImpl(%d)] Stats; \n", this->home_thread_id );

        for( auto &x : measurements){
            info(log_fp, "\t Sizeclass[%u] Time:%.2f Before:%u  After:%u \n", x.type, x.nanosec/1000.0,  x.before, x.after );
        }
    
    }


    void SendBlocksTo(CompactionCtx *ctx, helper_cb cb, void *owner);
    static void UpdateOwnershipTMsg(mts::thread_msg_t *msg);
    static void UpdateOwnershipMsgCb( void *owner);
    static void UpdateOwnershipMsgRetCb( mts::thread_msg_t *msg);
    static void UpdateOwnershipCb( void *owner);

    static void MergeBlocksTMsg(mts::thread_msg_t *msg);
    static void MergeBlocksMsgCb( void *owner);
    static void MergeBlocksMsgRetCb( mts::thread_msg_t *msg);
    static void FreePhysBlockMsgRetCb( mts::thread_msg_t *msg);
    void MergeBlocks(CompactionCtx *nctx,  helper_cb cb, void *owner);
  static void  DelayedCompactionCb(int rev, void *arg);
    void UnmapPointer(addr_t old_base_addr);

private:
    const int home_thread_id;
    BlockAlloc *sballoc;
    ibv_memory_manager *const ibv_rc;

    struct pending_alloc_t
    {

        thread_alloc_cb cb;
        void *owner;
    };

    struct CompactionMeasurement{
        uint64_t nanosec;
        uint8_t type;
        uint32_t before;
        uint32_t after;
    };

    std::list<CompactionMeasurement> measurements;

    // for allocations which wait for block allocation
    std::list<pending_alloc_t> pending_allocations[SizeTable::ClassCount - 1];

    // all_blocks allocated at this thread
    // even the compacted one, but they will point to new blocks
    std::unordered_map<addr_t, LocalBlock *> blocks;
    std::set<LocalBlock *, LocalBlockComp> not_full_blocks[SizeTable::ClassCount - 1]; // not_full_blocks at this thread can be list

    RandomGenerator gen;

    struct  BlockAllocCtx
    {
        BlockAllocCtx(ThreadAllocImpl *_this, uint8_t type):
            _this(_this), type(type), retry(0)
        {
            //empty
        }

        ThreadAllocImpl *_this;
        uint8_t type;
        int8_t retry;
    };

    struct  FixPointerCtx
    {
        FixPointerCtx(ThreadAllocImpl *_this, addr_t addr,  fixpointer_cb cb, void *owner ):
            _this(_this), addr(addr), cb(cb), owner(owner)
        {
            //empty
        }

        ThreadAllocImpl *_this;
        addr_t addr;
        fixpointer_cb cb;
        void *owner;
    };


    LocalBlock *GetFreeBlock(uint8_t type)
    {
        if (!not_full_blocks[type].empty())
        {
            auto it = not_full_blocks[type].begin(); // we take the most full block
            LocalBlock *b = *(it);
            not_full_blocks[type].erase(it);
            return b;
        }

        return nullptr;
    }




    void AllocBlock(uint8_t type, BlockAllocCtx *ctx);
    static void AllocBlockCb(Block *b,  addr_t addr, void *owner);
    static void DelayedAllocBlockCb(int rev, void *owner);

    void AllocBlockRet(Block *b, addr_t addr,  BlockAllocCtx *ctx) ;

    void AllocBlockFailedRet(BlockAllocCtx *ctx);
    void FailAllAllocations(uint8_t type);
    void BlockAllocDone(BlockAllocCtx *ctx) ;


    client_addr_t AllocFromBlock(LocalBlock *b);
    void AllocPendingFromBlock(LocalBlock *b);


    void FreeLocal(client_addr_t client_addr, thread_free_cb cb, void *owner);
    void FreeBlock(LocalBlock *block);
 
    static void FreeTMsg(mts::thread_msg_t *msg);
    static void FreeMsgCb(int status, addr_t newaddr, void *owner) ;
    static void FreeMsgRetCb(mts::thread_msg_t *msg);
    static void FreePhysBlockTMsg(mts::thread_msg_t *msg);


    static void AllocBlockTMsg(mts::thread_msg_t *msg);

    static void AllocBlockMsgCb(Block *block, addr_t addr, void *owner);

    static void AllocBlockMsgRetCb(mts::thread_msg_t *msg);

    static void FixPointerCb(addr_t newaddr, void *owner);

    void FixPointerLocal(client_addr_t addr,  fixpointer_cb cb, void *owner  );

    static void FixPointerTMsg(mts::thread_msg_t *msg);
    static void FixPointerMsgCb(int status, client_addr_t newaddr, void *owner);
    static void RemoveVirtAddrTMsgCb(void *owner);
    static void RemoveVirtAddrTMsg(mts::thread_msg_t *msg);

    static void DeallocateMessageRetCb(mts::thread_msg_t *msg);
  
    static void  FixPointerMsgRetCb(mts::thread_msg_t *msg);
    void Compact(LocalBlock *A, LocalBlock *B);
};

//Constructor

ThreadAllocImpl::ThreadAllocImpl(int  thread_id, BlockAlloc *sballoc, ibv_memory_manager *ibv_rc):
    home_thread_id(thread_id), sballoc(sballoc), ibv_rc(ibv_rc), gen(thread_id + 1)
{
    //generate 2 numbers
    gen.GetNewRandomNumber();
    gen.GetNewRandomNumber();
};

//-------------------------Alloc------------------------------//

void ThreadAllocImpl::Alloc(uint32_t user_size, thread_alloc_cb cb, void *owner)
{
    text(log_fp, "\t\t[ThreadAllocImpl(%u)] Get request to allocate user %" PRIu32" bytes\n", home_thread_id, user_size );
    uint8_t type = SizeTable::getInstance().GetClassFromUserSize(user_size);
    text(log_fp, "\t\t[ThreadAllocImpl(%u)] It assigned to type %" PRIu8" \n", home_thread_id, type );

    LocalBlock *block =  GetFreeBlock(type);
    if(block != nullptr)
    {
        text(log_fp, "\t\t[ThreadAllocImpl(%u)] Free block is found \n", home_thread_id );
        client_addr_t ret_addr = AllocFromBlock(block);

        if(!block->is_full())
        {
            // we allocate in the most full object, which is should be first
            not_full_blocks[type].emplace_hint(not_full_blocks[type].begin(), block);
        }

        cb(ret_addr, owner);
        return;
    }

    bool need_block = pending_allocations[type].empty();
    pending_allocations[type].push_back({.cb = cb, .owner = owner});

    if(need_block)
    {
        text(log_fp, "\t\t[ThreadAllocImpl(%u)] No Free block, Allocate a new one \n", home_thread_id );
        BlockAllocCtx *ctx = new BlockAllocCtx(this,  type);
        AllocBlock(type, ctx);
    }
    else
    {
        text(log_fp, "\t\t[ThreadAllocImpl(%u)] No Free block, Wait for a new one \n", home_thread_id );
    }
}


//-----------------------------------------------------------------------------
// allocate object when adapter is used

void ThreadAllocImpl::AllocAtHome(ThreadAlloc *home_alloc, uint32_t size, thread_alloc_cb cb, void *owner)
{

    text( log_fp, "\t\t[ThreadAllocImpl(%u)] Alloc via adapter, \n", home_thread_id);

    if(home_alloc == this)
    {
        Alloc(size, cb, owner);
        return;
    }
    else
    {
        text( log_fp, "\t\t[ThreadAllocImpl(%u)] Redirect \n", home_thread_id);
        mts::thread_msg_t *msg = new mts::thread_msg_t();
        msg->cb = &ThreadAllocImpl::AllocTMsg;
        msg->payload[0] = (void *)home_alloc;
        msg->payload[1] = (void *)(uint64_t)size;
        msg->payload[2] = (void *)cb;
        msg->payload[3] = (void *)owner;
        msg->payload[4] = (void *)this;
        mts::send_msg_to_thread_and_notify(home_alloc->GetHomeThreadMpIdx(), msg);
        return;
    }
}


void ThreadAllocImpl::AllocTMsg(mts::thread_msg_t *msg)
{
    ThreadAllocImpl *_this = (ThreadAllocImpl *)msg->payload[0];
    _this->Alloc((uint32_t)(uint64_t)msg->payload[1], AllocMsgCb, msg);

}

void ThreadAllocImpl::AllocMsgCb(client_addr_t ret_addr, void *owner)
{
    mts::thread_msg_t *msg = (mts::thread_msg_t *)owner;

    msg->cb = &ThreadAllocImpl::AllocMsgRetCb;
    msg->payload[0] = (void *)ret_addr.parts[0];
    msg->payload[1] = (void *)ret_addr.parts[1];

    ThreadAllocImpl *invoke_alloc = (ThreadAllocImpl *)msg->payload[4];
    mts::send_msg_to_thread_and_notify(invoke_alloc->GetHomeThreadMpIdx(), msg);
}


void ThreadAllocImpl::AllocMsgRetCb(mts::thread_msg_t *msg)
{

    uint64_t p1 = (addr_t)msg->payload[0];
    uint64_t p2 = (addr_t)msg->payload[1];
    client_addr_t ret_addr = {.parts = { p1, p2 } };
    thread_alloc_cb cb  = (thread_alloc_cb)msg->payload[2];
    void *owner = msg->payload[3];
    delete msg;

    cb(ret_addr, owner);
}


//-----------------------------------------------------------------------------
// allocate block

void ThreadAllocImpl::AllocBlock(uint8_t type, BlockAllocCtx *ctx)
{

    if(sballoc->GetHomeThreadMpIdx() == home_thread_id)
    {
        text(log_fp, "\t\t[ThreadAllocImpl(%u)] AllocBlock locally \n", home_thread_id );
        sballoc->AllocBlock(this, type, AllocBlockCb, ctx);
        return;
    }
    else
    {
        text(log_fp, "\t\t[ThreadAllocImpl(%u)] AllocBlock remotely \n", home_thread_id );
        mts::thread_msg_t *msg = new mts::thread_msg_t();
        msg->cb = &ThreadAllocImpl::AllocBlockTMsg;
        msg->payload[0] = (void *)sballoc;
        msg->payload[1] = (void *)this;
        msg->payload[2] = (void *)ctx;
        msg->payload[3] = (void *)(uint64_t)type;
        mts::send_msg_to_thread_and_notify(sballoc->GetHomeThreadMpIdx(), msg);
        return;
    }



    return;
}

void ThreadAllocImpl::AllocBlockTMsg(mts::thread_msg_t *msg)
{
    BlockAlloc *sballoc =  (BlockAlloc *)msg->payload[0];
    ThreadAlloc *alloc = (ThreadAlloc *)msg->payload[1];
    uint8_t type = (uint8_t)(uint64_t)msg->payload[3];
    sballoc->AllocBlock(alloc, type, AllocBlockMsgCb, msg);
}


void ThreadAllocImpl::AllocBlockMsgCb(Block *block, addr_t addr, void *owner)
{
    mts::thread_msg_t *msg = (mts::thread_msg_t *)owner;
    msg->cb = &ThreadAllocImpl::AllocBlockMsgRetCb;
    msg->payload[0] = (void *)block;
    msg->payload[3] = (void *)addr;
    ThreadAllocImpl *invoke_alloc = (ThreadAllocImpl *)msg->payload[1];
    mts::send_msg_to_thread_and_notify(invoke_alloc->GetHomeThreadMpIdx(), msg);
}


void ThreadAllocImpl::AllocBlockMsgRetCb(mts::thread_msg_t *msg)
{
    Block *block = (Block *)msg->payload[0];
    ThreadAllocImpl *_this = (ThreadAllocImpl *)msg->payload[1];
    BlockAllocCtx *ctx  = (BlockAllocCtx *)msg->payload[2];
    addr_t addr = (addr_t)msg->payload[3];
    delete msg;
    _this->AllocBlockRet(block, addr, ctx);
}



void ThreadAllocImpl::AllocBlockCb(Block *block, addr_t addr, void *owner)
{
    BlockAllocCtx *ctx = (BlockAllocCtx *)owner;
    ctx->_this->AllocBlockRet(block, addr, ctx);
}

void ThreadAllocImpl::AllocBlockRet(Block *block,  addr_t newaddr, BlockAllocCtx *ctx)
{

    if(newaddr == 0)
    {
        debug( log_fp, "\t\t[ThreadAllocImpl(%u)] block allocation failed item=%p\n", home_thread_id, ctx);
        AllocBlockFailedRet(ctx);
        return;
    }

    text( log_fp, "\t\t[ThreadAllocImpl(%u)] block allocation succeeded at phys addr = %d \n", home_thread_id, block->GetPhysAddr().fd);

    LocalBlock *locblock = new LocalBlock(this->gen, block, ctx->type, SizeTable::getInstance().GetRealSize(ctx->type) );

    struct ibv_mr *mr = ibv_rc->mem_reg_odp((void *)newaddr, block->GetSize());
    locblock->AddNewVirtAddr(mr);

    // add to the list of known blocks
    blocks.insert( {newaddr, locblock } );

    ReaderWriter::WriteBlockHeader(newaddr, ctx->type, mr->rkey);

    AllocPendingFromBlock(locblock);

    if(pending_allocations[ctx->type].empty())
    {
        // we are done
        BlockAllocDone(ctx);
    }
    else
    {
        // allocate a new block again
        AllocBlock(ctx->type, ctx);
    }
}

void ThreadAllocImpl::AllocBlockFailedRet(BlockAllocCtx *ctx)
{
    ctx->retry++;
    if(ctx->retry < 3)
    {
        mts::SCHEDULE_CALLBACK(GetHomeThreadMpIdx(), 2.4, DelayedAllocBlockCb, ctx);
    }
    else
    {
        FailAllAllocations(ctx->type);
        BlockAllocDone(ctx);
    }
}

void ThreadAllocImpl::FailAllAllocations(uint8_t type)
{
    while(!pending_allocations[type].empty())
    {
        client_addr_t ret_addr = {.parts = {0, 0}};
        pending_alloc_t temp = pending_allocations[type].front();
        temp.cb(ret_addr, temp.owner);
        pending_allocations[type].pop_front();
    }
}
void ThreadAllocImpl::DelayedAllocBlockCb(int rev, void *owner)
{
    BlockAllocCtx *ctx = (BlockAllocCtx *)owner;
    ctx->_this->AllocBlock(ctx->type, ctx);
}

void ThreadAllocImpl::BlockAllocDone(BlockAllocCtx *ctx)
{
    delete ctx;
}

// allocate objects from block

void ThreadAllocImpl::AllocPendingFromBlock(LocalBlock *block)
{
    uint8_t type = block->GetType();
    while(!pending_allocations[type].empty() && !block->is_full())
    {
        client_addr_t ret_addr = AllocFromBlock(block);
        pending_alloc_t temp = pending_allocations[type].front();
        temp.cb(ret_addr, temp.owner);
        pending_allocations[type].pop_front();
    }

    if(!block->is_full())
    {
        // we allocate in the most full object, which is should be first
        not_full_blocks[type].emplace_hint(not_full_blocks[type].begin(), block);
    }
}


client_addr_t ThreadAllocImpl::AllocFromBlock(LocalBlock *block)
{
    uint16_t unique_obj_id = 0;

    addr_t addr = block->GetBaseAddr() + block->AllocObject(&unique_obj_id);

    text(log_fp, "\t\t[ThreadAllocImpl(%u)] Allocate at address %" PRIx64" with id %" PRIu16 " \n", home_thread_id, addr, unique_obj_id);

    uint32_t rkey = block->GetRKey();
    uint8_t  type = block->GetType();

    text(log_fp, "\t\t[ThreadAllocImpl(%u)] Allocated from block of type %" PRIu8 " with rkey %" PRIu32 " \n", home_thread_id, type, rkey);

    assert(((addr & 0b111) == 0) && "Slot must be 8 bytes alligned");

    uint8_t version = 0;
    while(!ReaderWriter::SetNewObject(addr, unique_obj_id, &version ))
    {
        // should be impossible
        text(log_fp, ">>>>>>>>>>>>>>>>>>>[AllocFromBlock] Warning! Lock reacquire during alloc\n");
    }

//    uint64_t val = *(uint64_t*)((char*)addr);
//    info(log_fp, "\t\t object  %" PRIx64 " and obj id %" PRIx16"  \n",  val,unique_obj_id);

    client_addr_t ret_addr = CreateClientAddr(addr, rkey, unique_obj_id, version, type);
    return ret_addr;
}



//////////////////////////////////////////////////////////////////////////////


void ThreadAllocImpl::Free(client_addr_t client_addr,  thread_free_cb cb, void *owner)
{

    // map the address of the block to the thread on the local machine

    addr_t baseaddr = GetVirtBaseAddr(GetObjectAddr(client_addr));
    text( log_fp, "\t\t[ThreadAllocImpl(%u)] Free: virt base addr=%" PRIx64 ", \n", home_thread_id, baseaddr);
    ThreadAlloc *home_alloc = sballoc->GetHomeAlloc(baseaddr);
    if(home_alloc == this)
    {
        FreeLocal(client_addr, cb, owner);
        return;
    }
    else
    {

        mts::thread_msg_t *msg = new mts::thread_msg_t();
        msg->cb = &ThreadAllocImpl::FreeTMsg;
        msg->payload[0] = (void *)home_alloc;
        msg->payload[1] = (void *)client_addr.parts[0];
        msg->payload[2] = (void *)client_addr.parts[1];
        msg->payload[3] = (void *)cb;
        msg->payload[4] = (void *)owner;
        msg->payload[5] = (void *)this;
        mts::send_msg_to_thread_and_notify(home_alloc->GetHomeThreadMpIdx(), msg);
        return;
    }


}

void ThreadAllocImpl::FreeTMsg(mts::thread_msg_t *msg)
{
    ThreadAllocImpl *_this = (ThreadAllocImpl *)msg->payload[0];
    client_addr_t addr = {.parts = { (uint64_t)msg->payload[1], (uint64_t)msg->payload[2]} };
    _this->FreeLocal(addr, FreeMsgCb, msg);
}



void ThreadAllocImpl::FreeMsgCb(int status, addr_t newaddr, void *owner)
{
    mts::thread_msg_t *msg = (mts::thread_msg_t *)owner;

    msg->cb = &ThreadAllocImpl::FreeMsgRetCb;
    msg->payload[0] = (void *)newaddr;
    msg->payload[1] = (void *)(int64_t)status;
    ThreadAllocImpl *invoke_alloc = (ThreadAllocImpl *)msg->payload[5];
    mts::send_msg_to_thread_and_notify(invoke_alloc->GetHomeThreadMpIdx(), msg);
}


void ThreadAllocImpl::FreeMsgRetCb(mts::thread_msg_t *msg)
{

    addr_t newaddr = (addr_t)msg->payload[0];
    int  status = (uint8_t)(int64_t) msg->payload[1];
    thread_free_cb cb  = (thread_free_cb)msg->payload[3];
    void *owner = msg->payload[4];
    delete msg;
    cb(status, newaddr, owner);
}


void ThreadAllocImpl::FreeLocal(client_addr_t client_addr,  thread_free_cb cb, void *owner)
{
    addr_t base_addr = GetVirtBaseAddr(GetObjectAddr(client_addr));
    uint16_t obj_id = GetObjId(client_addr);
    text( log_fp, "\t\t[ThreadAllocImpl(%u)] FreeLocal: virt base addr=%" PRIx64 ", obj_id=%" PRIu16 " \n", home_thread_id, base_addr, obj_id);
 
    auto it = blocks.find(base_addr);

    if(it == blocks.end())
    {
        //block does not exist.  Can be a side effect of compaction. !!!!
        cb(BLOCK_IS_NOT_FOUND, 0, owner);
        return;
    }



    LocalBlock *block = it->second;
    uint8_t type = block->GetType();
    bool was_not_full = false;
    auto hint = not_full_blocks[type].begin();  // full objects are more likely to be added to beggining
    if (!block->is_full())
    {
        was_not_full = true;
        // erase it from list if it is not full as the set is ordered by the free space of blocks
        auto it = not_full_blocks[type].find(block);
        hint = not_full_blocks[type].erase(it);
    }

    offset_t offset = block->RemoveObject(obj_id); // remove object and get offset of the object

    // catch the case when obj is not removed as it does not exist
    if(offset == std::numeric_limits<offset_t>::max())
    {
        if(was_not_full)
        {
            not_full_blocks[type].emplace_hint(hint, block);
        }
        cb(OBJECT_DOES_NOT_EXIST, 0, owner);
        return;
    }

    assert(type == client_addr.comp.type);

    addr_t new_base_addr = block->GetBaseAddr();

    not_full_blocks[type].emplace_hint(hint, block);

    addr_t old_base_addr = ReaderWriter::FreeSlot(new_base_addr + offset);
    text( log_fp, "\t\t[ThreadAllocImpl(%u)] FreeLocal: actual obj_addr=%" PRIx64 " \n", home_thread_id, new_base_addr + offset);

    cb(SUCCESS, new_base_addr + offset, owner);
 
    bool can_unmap_old_addr = block->RemoveOneAddr(old_base_addr);
    if(can_unmap_old_addr){
        text( log_fp, "\t\t[ThreadAllocImpl(%u)] FreeLocal: unmap old base %" PRIx64 " \n", home_thread_id, old_base_addr);
        blocks.erase(it);
        struct ibv_mr* old_mr =  block->RemoveVirtAddr(old_base_addr);
        assert(old_mr->lkey==0 && "Should unbind all objects");
        old_mr->lkey = old_mr->rkey; // restore key for safety
        ibv_rc->mem_dereg(old_mr);
        UnmapPointer(old_base_addr);
    }
    // todo add block free if(block->is_free()) todo 
     
}

void ThreadAllocImpl::UnmapPointer(addr_t old_base_addr){
    if(sballoc->GetHomeThreadMpIdx() == home_thread_id)
    {
        sballoc->RemoveVirtAddr(old_base_addr, NULL, NULL);
    }
    else
    {
        mts::thread_msg_t *msg = new mts::thread_msg_t();
        msg->cb = &ThreadAllocImpl::RemoveVirtAddrTMsg;
        msg->payload[0] = (void *)sballoc;
        msg->payload[1] = (void *)old_base_addr;
        msg->payload[2] = (void *)this;
        mts::send_msg_to_thread_and_notify(sballoc->GetHomeThreadMpIdx(), msg);
    }
}
//////////////////// FOR reads /////////////////////////////////


void ThreadAllocImpl::FindObjectAddr(client_addr_t client_addr, thread_find_cb cb, void *owner)
{

    text( log_fp, "\t\t[ThreadAllocImpl(%u)] FindObjectAddr client addr=%" PRIx64 " \n", home_thread_id, client_addr.comp.addr);

    addr_t baseaddr = GetVirtBaseAddr(GetObjectAddr(client_addr));

    // 1) Check that the block address is valid at all
    ThreadAlloc *home_alloc = sballoc->GetHomeAlloc(baseaddr);
    if(home_alloc == nullptr)
    {
        cb(0, 0, owner);
        return;
    }

    // if is here the block exists. Find the object

    uint16_t slot_size = SizeTable::getInstance().GetRealSize(GetSlotType(baseaddr));
    addr_t newbaseaddr = GetSlotNewestBaseAddr(baseaddr);

    if(baseaddr != newbaseaddr) // if base is the same that the offset is 100% correct
    {
        text(log_fp, "\t\t[ThreadAllocImpl(%u)] Object has old base block.\n", home_thread_id);
    }

    uint16_t object_id = GetObjId(client_addr);
    uint16_t slot_object_id = GetSlotObjId(GetObjectAddr(client_addr));

    if(object_id == slot_object_id){
        addr_t newaddr = newbaseaddr + GetAddressOffset(GetObjectAddr(client_addr)); 
        cb(newaddr, slot_size, owner);
        return;
    }
 
    text(log_fp, "\t\t[ThreadAllocImpl] object %u has been moved %lu\n", object_id,   client_addr.comp.addr);
    if(home_alloc == this)
    {
        FindObjectLocal(client_addr, cb, owner);
        return;
    }

#ifdef SCAN_FIND

    text( log_fp, "\t\t[ThreadAllocImpl(%u)] Send message \n", home_thread_id);
    mts::thread_msg_t *msg = new mts::thread_msg_t();
    msg->cb = &ThreadAllocImpl::FindObjectTMsg;
    msg->payload[0] = (void *)home_alloc;
    msg->payload[1] = (void *)client_addr.parts[0];
    msg->payload[2] = (void *)client_addr.parts[1];
    msg->payload[3] = (void *)cb;
    msg->payload[4] = (void *)owner;
    msg->payload[5] = (void *)this;
    mts::send_msg_to_thread_and_notify(home_alloc->GetHomeThreadMpIdx(), msg);
    return;
   
#else

    text(log_fp, "\t\t[ThreadAllocImpl(%u)](Unsafe) Find object locally \n", home_thread_id);
 
    // this search is thread-independent and can be performed by any thread,
    //since we are sure that virtual address is valid
    addr_t newaddr = ReaderWriter::ScanMemory(newbaseaddr, slot_size, object_id ); // is linear search
    if(newaddr == 0LU)
    {
        info( log_fp, "\t\t[ThreadAllocImpl( )] The object is not found \n");
    }

    cb(newaddr, slot_size, owner);
    return;
#endif
}



void ThreadAllocImpl::FindObjectTMsg(mts::thread_msg_t *msg)
{
    ThreadAllocImpl *_this = (ThreadAllocImpl *)msg->payload[0];
    client_addr_t addr = {.parts = { (uint64_t)msg->payload[1], (uint64_t)msg->payload[2]} };
    _this->FindObjectLocal(addr, FindObjectMsgCb, msg);
}

void ThreadAllocImpl::FindObjectMsgCb(addr_t newaddr,  uint16_t slot_size,  void *owner)
{
    mts::thread_msg_t *msg = (mts::thread_msg_t *)owner;
    msg->cb = &ThreadAllocImpl::FindObjectRetCb;
    msg->payload[0] = (void *)(uint64_t)slot_size;
    msg->payload[1] = (void *)newaddr;
    ThreadAllocImpl *invoke_alloc = (ThreadAllocImpl *)msg->payload[5];
    mts::send_msg_to_thread_and_notify(invoke_alloc->GetHomeThreadMpIdx(), msg);
}


void ThreadAllocImpl::FindObjectRetCb(mts::thread_msg_t *msg)
{
    uint16_t slot_size = (uint16_t)(uint64_t)msg->payload[0];
    addr_t newaddr = (addr_t)msg->payload[1];
    thread_find_cb cb  = (thread_find_cb)msg->payload[3];
    void *owner = msg->payload[4];
    delete msg;
    cb(newaddr, slot_size, owner);
}

void ThreadAllocImpl::FindObjectLocal(client_addr_t client_addr, thread_find_cb cb, void *owner)
{
    addr_t base_addr = GetVirtBaseAddr(GetObjectAddr(client_addr));
    uint16_t obj_id = GetObjId(client_addr);
    auto it = blocks.find(base_addr);
   // assert(it != blocks.end() && "I don't have a block. Can be a side effect of compaction. !!!! ");
    if(it == blocks.end()){
        cb(0, 0, owner);
        return;
    }
    LocalBlock *block = it->second;

    offset_t offset = block->FindObject(obj_id); // remove object and get offset of the object

    uint16_t slot_size = block->GetSlotSize();
    addr_t new_base_addr = block->GetBaseAddr();

    text( log_fp, "\t\t[ThreadAllocImpl(%u)] FindObjectAddr: actual obj_addr=%" PRIx64 " \n", home_thread_id, new_base_addr + offset);;
    cb(new_base_addr + offset, slot_size, owner);
}



//////////////////// FOR fix pointer /////////////////////////////////

void ThreadAllocImpl::FixClientAddr(client_addr_t client_addr,  fixpointer_cb cb, void *owner  )
{
    text( log_fp, "\t\t[ThreadAllocImpl(%u)] FixClientAddr base addr=%" PRIx64 "\n", home_thread_id, client_addr.comp.addr);
    addr_t old_base_addr = GetVirtBaseAddr(GetObjectAddr(client_addr));
    ThreadAlloc *home_alloc = sballoc->GetHomeAlloc(old_base_addr);

    if(home_alloc == nullptr)
    {
        cb(NOT_FOUND, client_addr, owner);
        return;
    }
 
    if(home_alloc == this)
    {
        FixPointerLocal(client_addr, cb, owner);
        return;
    }
    else
    {

        mts::thread_msg_t *msg = new mts::thread_msg_t();
        msg->cb = &ThreadAllocImpl::FixPointerTMsg;
        msg->payload[0] = (void *)home_alloc;
        msg->payload[1] = (void *)client_addr.parts[0];
        msg->payload[2] = (void *)client_addr.parts[1];
        msg->payload[3] = (void *)cb;
        msg->payload[4] = (void *)owner;
        msg->payload[5] = (void *)this;
        mts::send_msg_to_thread_and_notify(home_alloc->GetHomeThreadMpIdx(), msg);
        return;
    }

}
 

void ThreadAllocImpl::FixPointerTMsg(mts::thread_msg_t *msg)
{
    ThreadAllocImpl *_this = (ThreadAllocImpl *)msg->payload[0];
    client_addr_t addr = {.parts = { (uint64_t)msg->payload[1], (uint64_t)msg->payload[2]} };
    _this->FixPointerLocal(addr, FixPointerMsgCb, msg);

}


void ThreadAllocImpl::FixPointerMsgCb(int status, client_addr_t newaddr, void *owner) 
{
    mts::thread_msg_t *msg = (mts::thread_msg_t *)owner;

    msg->cb = &ThreadAllocImpl::FreeMsgRetCb;
    msg->payload[0] = (void *)(int64_t)status;
    msg->payload[1] = (void *)newaddr.parts[0];
    msg->payload[2] = (void *)newaddr.parts[1];  
    ThreadAllocImpl *invoke_alloc = (ThreadAllocImpl *)msg->payload[5];
    mts::send_msg_to_thread_and_notify(invoke_alloc->GetHomeThreadMpIdx(), msg);
}


void ThreadAllocImpl::FixPointerMsgRetCb(mts::thread_msg_t *msg)
{
    int  status = (uint8_t)(int64_t) msg->payload[0];
    client_addr_t addr = {.parts = { (uint64_t)msg->payload[1], (uint64_t)msg->payload[2]} };
    fixpointer_cb cb  = (fixpointer_cb)msg->payload[3];
    void *owner = msg->payload[4];
    delete msg;
    cb(status, addr, owner);
}


 
void ThreadAllocImpl::DeallocateMessageRetCb(mts::thread_msg_t *msg)
{
    delete msg;
}


void ThreadAllocImpl::FixPointerLocal(client_addr_t client_addr,  fixpointer_cb cb, void *owner)
{
    addr_t base_addr = GetVirtBaseAddr(GetObjectAddr(client_addr));
    uint16_t obj_id = GetObjId(client_addr);

    text( log_fp, "\t\t[ThreadAllocImpl(%u)] FixPointerLocal base addr=%" PRIx64 " \n", home_thread_id, base_addr);
    auto it = blocks.find(base_addr);
    if(it == blocks.end())
    {
        text( log_fp, "\t\t[ThreadAllocImpl(%u)] Block was already fixed base addr=%" PRIx64 " \n", home_thread_id, base_addr);

        cb(BLOCK_IS_NOT_FOUND, client_addr, owner);
        return;
    }

    LocalBlock *block = it->second;
    offset_t new_offset = block->FindObject(obj_id);
    addr_t new_base_addr = block->GetBaseAddr();

    addr_t old_base_addr = ReaderWriter::FixOneSlot(new_base_addr + new_offset, obj_id, new_base_addr);
    if(old_base_addr == 0)
    {
        text( log_fp, "\t\t[ThreadAllocImpl(%u)] problem with fixing the slot\n", home_thread_id);
        cb(NOT_FOUND, client_addr, owner);
        return;
    }
 
    client_addr.comp.addr = new_base_addr + new_offset;
    client_addr.comp.rkey = block->GetRKey();

    cb(SUCCESS, client_addr, owner);

    bool can_unmap_old_addr = block->RemoveOneAddr(old_base_addr);
    if(can_unmap_old_addr){
        blocks.erase(it);
        struct ibv_mr* old_mr =  block->RemoveVirtAddr(old_base_addr);
        assert(old_mr->lkey==0 && "Should unbind all objects");
        old_mr->lkey = old_mr->rkey; // restore key for safety
        ibv_rc->mem_dereg(old_mr);
        UnmapPointer(old_base_addr);
    }
 
}

void ThreadAllocImpl::RemoveVirtAddrTMsg(mts::thread_msg_t *msg)
{
    BlockAlloc *sballoc =  (BlockAlloc *)msg->payload[0];
    addr_t addr = (addr_t)msg->payload[1];
    sballoc->RemoveVirtAddr(addr, RemoveVirtAddrTMsgCb, msg);
}

void ThreadAllocImpl::RemoveVirtAddrTMsgCb(void *owner)
{
    mts::thread_msg_t *msg = (mts::thread_msg_t *)owner;
    ThreadAllocImpl *invoke_alloc = (ThreadAllocImpl *)msg->payload[2];
    msg->cb = &ThreadAllocImpl::DeallocateMessageRetCb;
    mts::send_msg_to_thread_and_notify(invoke_alloc->GetHomeThreadMpIdx(), msg);
}

/////// compaction

void ThreadAllocImpl::Compaction(uint8_t type)
{
    // Conditions for compaction:
    // 1. Current thread owns blocks under compaction
    // 2. Blocks are compactable if Block B  has enough space to fit Block A

    if(not_full_blocks[type].size() < 2)
    {
        text(log_fp, "\t\t[ThreadAllocImpl(%u)] Not enough blocks to compact \n", home_thread_id );
        return;
    }

    text(log_fp, "\t\t[ThreadAllocImpl(%u)] Start compaction \n", home_thread_id );


    auto t1 = std::chrono::high_resolution_clock::now();

    // 1. Find candidate for merging. A-block
    // 2. Find a pair block to merge it to. B-bLock

    std::forward_list<LocalBlock *> failed_blocks;

    uint32_t original_size = (uint32_t)not_full_blocks[type].size() ; 
    uint32_t    counter = 0;

    uint32_t  compacted_objects = 0;
    // as an example just first two blocks.
    while(not_full_blocks[type].size() >= 2)  // compact as much as possible
    {
        auto A_it = std::prev(not_full_blocks[type].end()); // the most free
        LocalBlock *A = *A_it;
        not_full_blocks[type].erase(A_it);

        auto B_it = not_full_blocks[type].lower_bound(A->_allocatedslots); // find first block wich can fit the elements

        bool success = false;
        while(B_it != not_full_blocks[type].end()) // if there is no such object or it is itself we return
        {
            LocalBlock *B = *B_it;
            if(B->Compactible(A))  // can we add objects of A to B?
            {
                compacted_objects = A->hasObjects();
                text(log_fp, "\t\t[ThreadAllocImpl(%u)] Blocks are compactible \n", home_thread_id );
                not_full_blocks[type].erase(B_it); // B will be reinserted in Compact
                Compact(A, B); // insert objects of A to B
                success = true;
                break;
            }
            else
            {
                B_it++; // try next in the list
            }
        }

        if(!success)
        {
            text(log_fp, "\t\t[ThreadAllocImpl(%u)] Blocks are not compactible! \n", home_thread_id );
            failed_blocks.push_front(A);
        } else {
            if(compacted_objects > 0)
                 counter++;
            if(counter>6000){
  //              uint64_t ctxcompact = (((uint64_t)type) << 56) + ((uint64_t)(void*)this);
//                mts::SCHEDULE_CALLBACK(this->home_thread_id, 0.02, DelayedCompactionCb, (void*)ctxcompact); 
//                text(log_fp, "[MergeBlocks](%d) Compact blocks \n", this->home_thread_id);
		break;
            }
        }
    }

    // reinsert failed objects
    for (auto it = failed_blocks.begin(); it != failed_blocks.end(); ++it)
    {
        LocalBlock *lb = *it;
        not_full_blocks[type].insert(lb);
    }
 
    auto t2 = std::chrono::high_resolution_clock::now();
    uint64_t nanosec = std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - t1 ).count();
    uint32_t final_size = not_full_blocks[type].size();
    CompactionMeasurement m = {nanosec, type, original_size, final_size};
    this->measurements.push_back(m); 
}


// we compact A to B
// Conditions for compaction:
// 1. Current thread owns all blocks
// 2. Blocks are compactable if Block B has enough space to fit Block A
void ThreadAllocImpl::Compact(LocalBlock *A, LocalBlock *B)
{


    uint8_t type = A->GetType();
    assert(type == B->GetType() && "Blocks Must be of the same type! ");
    // 3. Copy data and metadata from A to B.
    B->AddEntriesFrom(A);

    // 4. Remap pointers.
    struct ibv_mr *mr = A->PopVirtAddr();
    while(mr != NULL)
    {

        // lookup addr to fix local pointer mapping
        addr_t addr_A = (uint64_t)mr->addr;
        text(log_fp, "\t\t[Compact(%u)] remap current addr %" PRIx64 " to block with addr  %" PRIx64 " ! \n", home_thread_id, addr_A, B->GetBaseAddr() );

        // fix the pointer
        auto blocksit = blocks.find(addr_A);
        assert(blocksit != blocks.end());
        blocksit->second = B;

        // remap addr and add it to a new block
        B->RemapVirtAddrToMe(addr_A);
        uint32_t counter = mr->lkey;
        mr->lkey = mr->rkey;
        ibv_rc->mem_rereg(mr);
        mr->lkey = counter;
        B->AddNewVirtAddr(mr);

        //pop next memory region
        mr = A->PopVirtAddr();
    }


    // 5. Free Block A
    _block_phys_addr_t phys_addr_A = A->GetPhysAddr();

    if(sballoc->GetHomeThreadMpIdx() == home_thread_id)
    {

        sballoc->FreePhysBlock(phys_addr_A, type);
    }
    else
    {
        mts::thread_msg_t *msg = new mts::thread_msg_t();
        msg->cb = &ThreadAllocImpl::FreePhysBlockTMsg;
        msg->payload[0] = (void *)sballoc;
        msg->payload[1] = (void *)(uint64_t)phys_addr_A.fd;
        msg->payload[2] = (void *)(uint64_t)phys_addr_A.offset_in_blocks;
        msg->payload[3] = (void *)(uint64_t)type;
        msg->payload[4] = (void *)this;
        mts::send_msg_to_thread_and_notify(sballoc->GetHomeThreadMpIdx(), msg);
    }

    // deallocate memory of the local block
    delete A;

    // pop block to not full blocks
    if(!B->is_full())
    {
        not_full_blocks[type].insert(  B );
    }

}

void ThreadAllocImpl::FreePhysBlockTMsg(mts::thread_msg_t *msg)
{
    BlockAlloc *sballoc =  (BlockAlloc *)msg->payload[0];
    _block_phys_addr_t phys_addr_A = { .fd = (int)(uint64_t)msg->payload[1], .offset_in_blocks = (uint32_t)(uint64_t)msg->payload[2] };
    uint8_t type = (uint8_t)(uint64_t)msg->payload[3];
    sballoc->FreePhysBlock(phys_addr_A, type);
    msg->cb = &ThreadAllocImpl::FreePhysBlockMsgRetCb;
    ThreadAllocImpl *invoke_alloc = (ThreadAllocImpl *)msg->payload[4];
    mts::send_msg_to_thread_and_notify(invoke_alloc->GetHomeThreadMpIdx(), msg);
}

void ThreadAllocImpl::FreePhysBlockMsgRetCb( mts::thread_msg_t *msg)
{
    delete msg;
}


///////---------------   block collocation

// the call finds blocks of type and send them to the master
void ThreadAllocImpl::SendBlocksTo(CompactionCtx *ctx, helper_cb cb, void *owner)
{


    uint32_t max_blocks = 2;
    uint8_t type = ctx->type;

    for(uint32_t i = 0; i < max_blocks; i++ )
    {
        if(not_full_blocks[type].empty())
        {
            break;
        }
        auto lastit = std::prev(not_full_blocks[type].end()); // the most free
        LocalBlock *b = *lastit;
        blocks.erase( b->GetBaseAddr() );
        not_full_blocks[type].erase(lastit);
        ctx->q.push(b); // push to concurrent q
    }


    uint8_t val = (--(ctx->counter));
    if(val == 0 )  // I am the last
    {
        text(log_fp, "\t\t[SendBlocksTo](%d) Last worker\n", this->home_thread_id);

        if ( ctx->q.empty() )
        {
            text(log_fp, "\t\t[SendBlocksTo](%d) No blocks has found\n", this->home_thread_id);
            // we are done with compactions. We have not found any block.
            cb(owner); // calls back the caller to deallocate memory
            return;
        }
        else
        {

            if( this == ctx->master )
            {
                MergeBlocks(ctx, cb, owner);
            }
            else
            {
                text(log_fp, "\t\t[SendBlocksTo](%u)send context to compaction master \n",this->home_thread_id);
                mts::thread_msg_t *msg = new mts::thread_msg_t();
                msg->cb = &ThreadAllocImpl::MergeBlocksTMsg;
                msg->payload[0] = (void *)ctx;
                msg->payload[1] = (void *)this;
                msg->payload[2] = (void *)cb;
                msg->payload[3] = (void *)owner;
                mts::send_msg_to_thread_and_notify(ctx->master->GetHomeThreadMpIdx(), msg);
            }
        }
    }
}

void ThreadAllocImpl::MergeBlocksTMsg(mts::thread_msg_t *msg)
{
    CompactionCtx *ctx = (CompactionCtx *)msg->payload[0];
    ThreadAllocImpl *_this = (ThreadAllocImpl *)ctx->master;
    _this->MergeBlocks(ctx, MergeBlocksMsgCb, msg);
}


void ThreadAllocImpl::MergeBlocksMsgCb(void *owner)
{
    mts::thread_msg_t *msg = (mts::thread_msg_t *)owner;
    msg->cb = &ThreadAllocImpl::MergeBlocksMsgRetCb;
    ThreadAllocImpl *invoke_alloc = (ThreadAllocImpl *)msg->payload[1];
    mts::send_msg_to_thread_and_notify(invoke_alloc->GetHomeThreadMpIdx(), msg);
}



void ThreadAllocImpl::MergeBlocksMsgRetCb( mts::thread_msg_t *msg)
{

    helper_cb cb = (helper_cb)msg->payload[2];
    void *owner = msg->payload[3];
    delete msg;
    cb(owner);
}

void ThreadAllocImpl::DelayedCompactionCb(int rev, void *arg)
{
    text(log_fp, "\t [DelayedFindObjectAddrCb] \n");
    uint64_t ctxcompact = (uint64_t)arg;
    ThreadAllocImpl *_this = ( ThreadAllocImpl *)(void*)( ctxcompact & mask_of_bits(56));
    uint8_t type = (uint8_t)(ctxcompact >> 56);
    _this->Compaction(type);
}

void ThreadAllocImpl::MergeBlocks( CompactionCtx *ctx,  helper_cb cb, void *owner)
{

    text(log_fp, "[MergeBlocks](%d) Merge blocks \n", this->home_thread_id);
 
    std::forward_list<addr_t> *list = new std::forward_list<addr_t>();

    LocalBlock *b;
    uint8_t type = 0;
    while(ctx->q.pop(b))
    {
        b->GetAllAddrs(list);
 
        type = b->GetType();
        blocks[b->GetBaseAddr()] = b;
        not_full_blocks[type].insert(b);
    }
 
 
    if(ctx->with_compaction)
    {
        uint64_t ctxcompact = (((uint64_t)type) << 56) + ((uint64_t)(void*)this);
        mts::SCHEDULE_CALLBACK(this->home_thread_id, 0.2, DelayedCompactionCb, (void*)ctxcompact); 
        text(log_fp, "[MergeBlocks](%d) Compact blocks \n", this->home_thread_id);
    }
 
    assert(ctx->master == this);
    cb(owner);
 
    // update ownership of blocks at sballoc
 
    if(sballoc->GetHomeThreadMpIdx() == home_thread_id)
    {
        // I am the owner of sballoc and can update ownership
        sballoc->UpdateOwnership(list, this, NULL, NULL);
        delete list;
    }
    else
    {
        mts::thread_msg_t *msg = new mts::thread_msg_t();
        msg->cb = &ThreadAllocImpl::UpdateOwnershipTMsg;
        msg->payload[0] = (void *)sballoc;
        msg->payload[1] = (void *)list;
      //  msg->payload[3] = (void *)master;
        msg->payload[2] = (void *)this;
        mts::send_msg_to_thread_and_notify(sballoc->GetHomeThreadMpIdx(), msg);
    }
 
}


void ThreadAllocImpl::UpdateOwnershipTMsg(mts::thread_msg_t *msg)
{
    BlockAlloc *sballoc = (BlockAlloc *)msg->payload[0];
    std::forward_list<addr_t> *addresses  = (std::forward_list<addr_t>  *)msg->payload[1];
     
    ThreadAlloc *master = (ThreadAlloc *)msg->payload[2];

    sballoc->UpdateOwnership(addresses,  master, UpdateOwnershipMsgCb, msg);
}


void ThreadAllocImpl::UpdateOwnershipMsgCb( void *owner)
{
    mts::thread_msg_t *msg = (mts::thread_msg_t *)owner;
    msg->cb = &ThreadAllocImpl::UpdateOwnershipMsgRetCb;
    ThreadAllocImpl *invoke_alloc = (ThreadAllocImpl *)msg->payload[2];
    mts::send_msg_to_thread_and_notify(invoke_alloc->GetHomeThreadMpIdx(), msg);
}

void ThreadAllocImpl::UpdateOwnershipMsgRetCb( mts::thread_msg_t *msg)
{
    std::forward_list<addr_t> *addresses  = (std::forward_list<addr_t>  *)msg->payload[1];
    delete addresses;
    delete msg;
}



