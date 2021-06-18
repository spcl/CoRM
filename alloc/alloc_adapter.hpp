/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * Allocation adapter that helps redirect allocation requests to threads depending on size-class 
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */
#pragma once

#include "size_table.hpp"
#include "alloc.hpp"
#include <atomic> 
#include <vector>
#include "../thread/messenger.hpp"
 
/// alloc adapter is also responsible for triggering compaction 
class AllocAdapter{
private:

    AllocAdapter(uint32_t threshold_popular_class, uint32_t threshold_size_class);

    static AllocAdapter& getInstanceImpl(uint32_t threshold_popular_class = 0, uint32_t threshold_size_class = 0)
    {
        static AllocAdapter instance{ threshold_popular_class, threshold_size_class};
        return instance;
    }

public:
    static AllocAdapter& getInstance()
    {
        return getInstanceImpl();
    }

    static void init(uint32_t threshold_popular_class, uint32_t threshold_size_class)
    {
       getInstanceImpl(threshold_popular_class,threshold_size_class );
    }

    AllocAdapter(AllocAdapter const&)    = delete;
    void operator=(AllocAdapter const&)  = delete;

 
public:
    void RegThread(ThreadAlloc* t, uint32_t id);
    std::atomic<uint64_t>* GetBstats( );


    void Alloc(uint32_t home_thread_id, uint32_t user_size, void *owner);
    void Free(uint32_t home_thread_id, client_addr_t client_addr,  void *owner);


    void processAllocReply(client_addr_t ret_addr);
    void processFreeReply(int status, uint8_t type );

    static void AllocReplyCb( client_addr_t ret_addr, void *owner);
    static void FreeReplyCb(int status, addr_t newaddr, void *owner);

    static void CompactionTmsg(mts::thread_msg_t *tmsg);
    static void CompactionMsgCb(void *owner);

    static void CompactionMsgRetCb(mts::thread_msg_t *msg);

    void print_stats(){
        info(log_fp, "[AllocAdapter] Stats; Compaction times in us:\n");

        for(uint32_t i = 0; i<SizeTable::ClassCount; i++)
        {   
            if(compaction_statistics[i].empty()){
                continue;
            }
            info(log_fp, "\t SizeClass[%u]: \n", i );
            for(auto &x:  compaction_statistics[i]){
                info(log_fp, "%.2f ", (x / 1000.0)) ;
            }
            info(log_fp, "\n" ) ;
        }       
    }
//private:
 
    std::atomic<uint64_t> alloc_stats[SizeTable::ClassCount];
    std::atomic<uint64_t> bstat[SizeTable::ClassCount]; 
    std::atomic<bool> pending[SizeTable::ClassCount];
    std::vector<ThreadAlloc*> all_allocs;

    std::vector<std::vector<uint64_t>> compaction_statistics;
 
    const uint32_t num_threads;
    const uint32_t threshold_popular_class;
    const uint32_t threshold_size_class; 
 
    ThreadAlloc* getAlloc(uint32_t id);
    uint32_t get_thread_id(uint32_t size , uint32_t home_thread_id);
    uint8_t get_compaction_master(uint8_t type);
    void trigger_type_collection(uint8_t type, bool with_compaction = false);
    int get_best_compaction_candidate();
    void finishCompaction(ThreadAlloc::CompactionCtx* ctx);
};


/*****************************************************************************
    
                    Implementation of alloc adapter           

******************************************************************************/



AllocAdapter::AllocAdapter(uint32_t threshold_popular_class, uint32_t threshold_size_class): 
    num_threads(mts::num_threads), threshold_popular_class(threshold_popular_class),threshold_size_class(threshold_size_class)
{   
    all_allocs.resize(num_threads);
    for(uint32_t i = 0; i<SizeTable::ClassCount; i++)
    {
        alloc_stats[i] = 0;
        pending[i] = false;
        bstat[i] = 0;
        compaction_statistics.push_back({});
    }       
}

void AllocAdapter::RegThread(ThreadAlloc* t, uint32_t id){
    all_allocs[id] = t;
}

std::atomic<uint64_t>* AllocAdapter::GetBstats(){
    return this->bstat;
}
 

uint32_t AllocAdapter::get_thread_id(uint32_t user_size, uint32_t  home_thread_id){
    //assert(0 && "Not implemented");
    uint8_t type = SizeTable::getInstance().GetClassFromUserSize(user_size);
 
    if(type < threshold_size_class || alloc_stats[type].load(std::memory_order_relaxed) > threshold_popular_class ){
        return home_thread_id;
    }

    text(log_fp, "Redirect alloc to another thread! \n");
    return type % num_threads;
}


void AllocAdapter::Alloc(uint32_t home_thread_id, uint32_t user_size, void *owner){
    uint32_t thread_id = get_thread_id(user_size, home_thread_id);
    all_allocs[home_thread_id]->AllocAtHome(all_allocs[thread_id], user_size, AllocReplyCb, owner);
}



void AllocAdapter::processAllocReply( client_addr_t ret_addr ) {
      // for debugging. To activate one compaction after 5 allocs
  /*  if( alloc_stats[ret_addr.comp.type] > 5 &&  !( pending[ret_addr.comp.type].load(std::memory_order_relaxed) ) &&  num_threads > 1 ){  

        text(log_fp, "(%d) Trigger collection! \n", mts::thread_id);
        trigger_type_collection(ret_addr.comp.type, true);
    } */
     
    if( ret_addr.comp.addr == (0ULL) ){
        int ret = get_best_compaction_candidate();
        if(ret < 0){
            //we run out of memory 
            if (num_threads > 1 ){
                // we will enforce one thread to have the class and allocate from that thread
                // This compaction will collect all not full blocks at one thread and ask to allocate the object
                trigger_type_collection(ret_addr.comp.type, false);
            }
        } else {
            // trigger compaction to find room for allocation
            uint8_t type = (uint8_t)ret;
            if( !pending[type].load(std::memory_order_relaxed) ){

                if(num_threads > 1){
                    trigger_type_collection(ret_addr.comp.type, true);
                }else{
                    all_allocs[0]->Compaction(ret_addr.comp.type);
                }
            }
        }
    } else {
        alloc_stats[ret_addr.comp.type]++;
    }
}

 // This compaction will collect all not full blocks at one thread and ask to allocate the object
void AllocAdapter::trigger_type_collection(uint8_t type, bool with_compaction ){
    bool expected = false;
    bool exchanged = pending[type].compare_exchange_strong(expected, true);

    if(!exchanged){
        // only one collection at a time
        return;
    }
 
    text(log_fp, "(%d) Prepare compaction ctx! \n", mts::thread_id);


    uint8_t compaction_master = get_compaction_master(type); // the thread which will gather the blocks
    text(log_fp, "(%d) compaction_master for type %u is %u! \n",  mts::thread_id,type, compaction_master);

    ThreadAlloc::CompactionCtx* ctx = new ThreadAlloc::CompactionCtx(num_threads, type, mts::thread_id, all_allocs[compaction_master], with_compaction);

    mts::thread_msg_t *tmsg = new mts::thread_msg_t();
    tmsg->cb = &CompactionTmsg; 
    tmsg->payload[0] = ctx;

    for(uint32_t thread_id = 0; thread_id < num_threads; thread_id++){
        if(thread_id!=compaction_master && thread_id!=mts::thread_id){
            mts::send_msg_to_thread_and_notify(thread_id, tmsg);
            text(log_fp, "Send ctx to thread ! %d \n", thread_id);
        }
    }

    if( mts::thread_id != compaction_master){
        text(log_fp, "SendBlocksTo locally ctx to thread ! %d \n", mts::thread_id);
        all_allocs[mts::thread_id]->SendBlocksTo(ctx, CompactionMsgCb, tmsg); // there is a chance that this thread will be the last
    }
}

uint8_t AllocAdapter::get_compaction_master(uint8_t type){
    return type % num_threads;
}

ThreadAlloc* AllocAdapter::getAlloc(uint32_t id) {
    return all_allocs[id];
}

void AllocAdapter::CompactionTmsg(mts::thread_msg_t *tmsg){
    ThreadAlloc* alloc = AllocAdapter::getInstance().getAlloc(mts::thread_id);
    ThreadAlloc::CompactionCtx* ctx = (ThreadAlloc::CompactionCtx*)(tmsg->payload[0]);
    alloc->SendBlocksTo(ctx, CompactionMsgCb, tmsg);
}



void AllocAdapter::CompactionMsgCb(void *owner){
    mts::thread_msg_t *msg = (mts::thread_msg_t *)owner;
    ThreadAlloc::CompactionCtx* ctx = (ThreadAlloc::CompactionCtx*)msg->payload[0];

    if(ctx->initiator != mts::thread_id){
        msg->cb = &AllocAdapter::CompactionMsgRetCb;
        mts::send_msg_to_thread_and_notify(ctx->initiator, msg);
        return;
    }

    CompactionMsgRetCb(msg);
}

void AllocAdapter::CompactionMsgRetCb(mts::thread_msg_t *msg){
   
    ThreadAlloc::CompactionCtx* ctx = (ThreadAlloc::CompactionCtx*)msg->payload[0];
    text(log_fp,"Collection is completed for class %u %s \n", ctx->type, ctx->with_compaction ? "with compaction": "");
 
    AllocAdapter::getInstance().finishCompaction(ctx);
 
    delete ctx;
    delete msg;
}

void AllocAdapter::finishCompaction( ThreadAlloc::CompactionCtx* ctx){
    // measure latency
    auto t2 = std::chrono::high_resolution_clock::now(); 
    uint64_t nanosec = std::chrono::duration_cast< std::chrono::nanoseconds >( t2 - ctx->t1 ).count();
    compaction_statistics[ctx->type].push_back(nanosec);
    // measure latency

    pending[ctx->type].store( false );  
}


 
int AllocAdapter::get_best_compaction_candidate(){
    std::vector<float> res;
    res.resize(SizeTable::ClassCount);

    int type = -1;
    float best_score = 0;

    for( uint32_t i=0; i < SizeTable::ClassCount; i++){
        uint64_t blocks_allocated = bstat[i].load(std::memory_order_relaxed);

        if( blocks_allocated && !( pending[i].load(std::memory_order_relaxed) ) ){
            float score =  1.0 - 
                            (alloc_stats[i].load(std::memory_order_relaxed) + 0.0) / 
                            (blocks_allocated * SizeTable::getInstance().objects_per_class[i] ) ; 
            if(score >best_score ){
                best_score = score;
                type = i;
            }
        }
    }

    if(best_score > 0.3){ // some threshold
        return type;
    }

    return -1;
}
 
void AllocAdapter::Free(uint32_t home_thread_id, client_addr_t client_addr,  void *owner){
    all_allocs[home_thread_id]->Free(client_addr, FreeReplyCb, owner);
}



void  AllocAdapter::processFreeReply(int status, uint8_t type){
    if(status==SUCCESS){
        alloc_stats[type]--;
    }
}
