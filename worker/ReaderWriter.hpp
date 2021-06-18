/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * The function for reading and writing objects to the memory.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */
#pragma once

 
#include <stdio.h>
#include <string.h>
#include "../common/common.hpp"
#include <algorithm>

typedef void (*read_cb)(uint16_t size, addr_t newaddr,uint8_t version, void *owner);
typedef void (*write_cb)( addr_t newaddr, uint8_t success, uint8_t version, void *owner);

static const int OBJECT_IS_INCONSISTENT = -4;
static const int NOT_FOUND = -3;
static const int UNDER_COMPACTION = -2;
static const int LOCK_FAILED = -1;


class ReaderWriter{
    

public:
    /*The call copies data to reply_buffer from slot at address pointer*/
    static void ReadObjectToBuf(char* reply_buffer, addr_t pointer,  uint16_t obj_id, uint16_t slot_size, read_cb cb , void* owner );
    // ReadObjectToBuf internally calls read_object_to_buffer, which is lock-free.
    // read_object_to_buffer knows how to read object splitted with cacheline versions
    static int read_object_to_buffer(uint64_t to, uint64_t from, uint16_t obj_id, uint16_t slot_size, uint16_t *return_size, uint8_t *version );
    // read_object_to_buffer_lim is similar to read_object_to_buffer, but also takes into acccount that destination buffer can be small
    static int client_read_object_to_buffer_lim(uint64_t to, uint64_t from, uint64_t actual_remote_address,  uint16_t slot_size, uint16_t obj_id, uint32_t *lim_size );

    // as above but no objid check
    static int client_read_object_farm(uint64_t to, uint64_t from, uint64_t actual_remote_address,  uint16_t slot_size, uint32_t *lim_size );
    static int client_read_fast(uint64_t to, uint64_t from,uint16_t slot_size, uint32_t *lim_size );

    // WriteBufToObject will write data to slot. It will try to lock object first and then write
    static void WriteBufToObject(addr_t pointer,  uint16_t obj_id, uint16_t slot_size, void *buf, uint32_t size, write_cb cb , void* owner );
    // WriteBufToObjectAtomic is similar to previous one, but it will also check the expected version before locking.
    static void WriteBufToObjectAtomic(addr_t pointer,  uint16_t obj_id,  uint16_t slot_size, uint8_t version, void *buf, uint32_t size, write_cb cb , void* owner );
    static void write_locally(addr_t localbuf, uint8_t new_version, uint16_t slot_size, void* buf,  uint32_t bufsize);
    
    // the call fixes object addresses 
    static void FixSlots(addr_t old_client_addr, addr_t newaddr, uint16_t slot_size);    
    static addr_t FixOneSlot(addr_t addr,uint16_t obj_id, addr_t new_base_addr);
    static bool HasOldBaseSlot(addr_t old_base_addr, addr_t new_base_addr,uint16_t slot_size);


    // different calls for locking and unlocking objects
    static int try_lock_slot_and_increment_version(addr_t localbuf, uint16_t obj_id);
    static int try_lock_slot_and_increment_version_if_version(addr_t localbuf, uint16_t obj_id, uint8_t version);
    static bool try_lock_slot_for_compaction(addr_t localbuf);
    static void unlock_slot(addr_t localbuf);
    static bool unlock_slot_from_compaction(addr_t localbuf);

    // write block header for new blocks
    static void WriteBlockHeader(addr_t addr, uint8_t type, uint32_t rkey);


    // scan memory block to find an object with object_id. it is linear
    static addr_t ScanMemory(addr_t addr, uint16_t slot_size, uint16_t object_id );
    // set object version for new objects.
    static bool SetNewObject(addr_t addr, uint16_t object_id, uint8_t *version);
    // clear object header at address
    static addr_t FreeSlot(addr_t addr);
};


void ReaderWriter::WriteBlockHeader(addr_t addr, uint8_t type, uint32_t rkey) {
    memset((char*)addr,0,BLOCK_SIZE);
    block_header_t* h = (block_header_t*)(addr + BLOCK_USEFUL_SIZE);
    h->comp.rkey = rkey;
    h->comp.type = type;
    h->comp.base = addr; //(addr >> BLOCK_BIT_SIZE);
}

// TODO. is not safe, if user is freeing an ibject in use.
addr_t ReaderWriter::FreeSlot(addr_t addr){
    text(log_fp, "\t\t[ReaderWriter] zero slot  at %" PRIx64 "\n",addr);

    std::atomic<slot_header_t> * head = (std::atomic<slot_header_t> *)addr;
    slot_header_t header_copy = head->load(std::memory_order_relaxed);

    if(header_copy.lock){
        return 0;
    }
    addr_t old_base = header_copy.oldbase;
    slot_header_t deallocated_header_copy = header_copy;
    deallocated_header_copy.allocated = 0;
    deallocated_header_copy.obj_id = 0;
    deallocated_header_copy.oldbase = 0;

    bool suc =  head->compare_exchange_strong(header_copy,deallocated_header_copy);
    if(suc)
        return (old_base << BLOCK_BIT_SIZE);
    else
        return 0;
}


bool ReaderWriter::SetNewObject(addr_t addr, uint16_t object_id, uint8_t *version){
    text(log_fp, "\t\t[ReaderWriter] SetNewObject at %" PRIx64 "\n",addr);

    std::atomic<slot_header_t> * head = (std::atomic<slot_header_t> *)addr;
    slot_header_t header_copy = head->load(std::memory_order_relaxed);

    if(header_copy.lock){
        return false;
    }

    slot_header_t allocated_header_copy = header_copy;

    allocated_header_copy.allocated = 1;
    allocated_header_copy.obj_id = object_id;
    allocated_header_copy.oldbase = (addr >> BLOCK_BIT_SIZE);

    *version =  allocated_header_copy.version;

    return head->compare_exchange_strong(header_copy,allocated_header_copy);
} 

 
  
void ReaderWriter::write_locally(addr_t localbuf, uint8_t new_version, uint16_t slot_size, void* buf,  uint32_t bufsize){
    text(log_fp, "\t\t[ReaderWriter] object at 0x%" PRIx64 " \n", localbuf);
    uint64_t next_cache_line = (localbuf & CACHELINE_MASK) + CACHELINE; //
    text(log_fp, "\t\t[ReaderWriter] next cache line at 0x%" PRIx64 " \n", next_cache_line);
    uint64_t src = (uint64_t)buf;
    uint64_t dst = localbuf + sizeof(slot_header_t);
    text(log_fp, "\t\t[ReaderWriter] data at 0x%" PRIx64 " \n", dst);
    uint64_t tocopy = bufsize;
    assert(next_cache_line >= dst && "cacheline overlapped header! ");

    uint64_t copy_size = std::min(next_cache_line-dst, tocopy); 
    text(log_fp, "\t\t[ReaderWriter] can copy before first cacheversion %" PRIu64 " \n", copy_size);
    memcpy((char*)dst, (char*)src, copy_size);
    src  += copy_size;
    dst  += copy_size;
    tocopy -= copy_size;

    while(tocopy > 0){
        text(log_fp, "\t\t[ReaderWriter] write version to cache at 0x%" PRIx64 " \n", dst);
        *(uint8_t*)dst = new_version;
        dst++;
        copy_size = std::min(CACHELINE -1, tocopy);
        text(log_fp, "\t\t[ReaderWriter] can copy before next cacheversion %" PRIu64 " \n", copy_size);
        memcpy((char*)dst, (char*)src, copy_size);
        src  += copy_size;
        dst  += copy_size;
        tocopy -= copy_size;
    }

    assert(dst <= localbuf+slot_size && "Overflow write");

    // jump to next cacheline
    dst = (dst + CACHELINE -1) &  CACHELINE_MASK;
    text(log_fp, "\t\t[ReaderWriter] jumped to  %" PRIx64 " \n", dst);

    //
    while( dst < localbuf+slot_size ){ 
        text(log_fp, "\t\t[ReaderWriter] write cache version to %" PRIx64 " \n", dst);
        *(uint8_t*)dst = new_version;
        dst+=CACHELINE;
    } 

    text(log_fp, "\t\t[ReaderWriter] write_locally finished \n");
}

void ReaderWriter::ReadObjectToBuf(char* reply_buffer, addr_t newaddr, uint16_t obj_id, uint16_t slot_size, read_cb cb , void* owner ){
    text(log_fp, "\t\t[ReaderWriter] Read Object To Buffer from  %" PRIx64 "\n ", newaddr );
    
    uint16_t size = 0;
    uint8_t version = 0;
    uint8_t try_count = 0;

    int ret = read_object_to_buffer((uint64_t)reply_buffer, newaddr, obj_id, slot_size, &size,&version);

    while(ret < 0){
        try_count++;
        text(log_fp, "\t\t[ReaderWriter] retry read %u \n",try_count );
        if(ret==NOT_FOUND || ret == UNDER_COMPACTION){ // object is invalid. need to find address again
            info(log_fp, "\t\t[ReadObjectToBuf] failed to read  since compaction\n" );
            cb(0, 0, 0, owner ); 
            return;
        }
        if(try_count > 4 ){ // lock failed
            info(log_fp, "\t\t[ReadObjectToBuf] failed to read \n" );
            cb(0, newaddr, 0, owner ); 
            return; 
        }
        ret = read_object_to_buffer((uint64_t)reply_buffer, newaddr, obj_id, slot_size, &size,&version); 
    }
 
    text(log_fp, "\t\t[ReaderWriter] We read  %" PRIu64 "\n", (*reinterpret_cast<uint64_t*>(reply_buffer)) );
    text(log_fp, "\t\t[ReaderWriter] read from %p \n", reply_buffer);
    cb(size, newaddr, version, owner ); 
    return ;
}


void ReaderWriter::WriteBufToObjectAtomic(addr_t newaddr, uint16_t obj_id,  uint16_t slot_size, uint8_t version, void *buf, uint32_t size, write_cb cb , void* owner ){
    text(log_fp, "\t\t[ReaderWriter] Write Object From Buffer to  %" PRIx64 "\n", newaddr );
 
    uint8_t try_count = 0;

    int ret = try_lock_slot_and_increment_version_if_version(newaddr, obj_id, version);

    while(ret < 0){
        try_count++;
        text(log_fp, "\t\t[WriteBufToObjectAtomic] retry lock %u \n",try_count );
        if(ret==NOT_FOUND || ret == UNDER_COMPACTION){ // object is invalid. need to find address again
            cb( 0, 0, 0,owner ); 
            return;
        }

        if(ret==OBJECT_IS_INCONSISTENT){
            cb( newaddr, 2, 0, owner ); 
        }

        if(try_count > 4 ){ // lock failed
            cb( newaddr, 0, 0, owner ); 
            return; 
        }

        ret = try_lock_slot_and_increment_version_if_version(newaddr, obj_id, version);
    }

 
    uint8_t newversion = (uint8_t)ret;
    write_locally(newaddr, newversion, slot_size, buf, size);
    unlock_slot(newaddr);
    text(log_fp, "\t\t[ReaderWriter] We wrote  %" PRIu64 "\n", (*static_cast<uint64_t*>(buf)) );

    cb(newaddr,  1 , newversion ,owner ); 
    return;
}



void ReaderWriter::WriteBufToObject(addr_t newaddr,  uint16_t obj_id,  uint16_t slot_size, 
                                  void *buf, uint32_t size, write_cb cb , void* owner ){
    text(log_fp, "\t\t[ReaderWriter] Write Object From Buffer to  %" PRIx64 "\n", newaddr );
 
    uint8_t try_count = 0;

    int ret = try_lock_slot_and_increment_version(newaddr, obj_id);

    while(ret < 0){
        try_count++;
        text(log_fp, "\t\t[WriteBufToObject] retry lock %u \n",try_count );
        if(ret==NOT_FOUND || ret == UNDER_COMPACTION){ // object is invalid. need to find address again
            if (cb) cb( 0, 0, 0,owner ); 
            info(log_fp, "\t\t[WriteBufToObject] failed because of compaction \n");
            return;
        }
 
        if(try_count > 4 ){ // lock failed
            info(log_fp, "\t\t[WriteBufToObject] failed because of lock \n");
            if (cb) cb( newaddr, 0, 0, owner ); 
            return; 
        }

        ret = try_lock_slot_and_increment_version(newaddr, obj_id);
    }
    uint8_t newversion = (uint8_t)ret;
    write_locally(newaddr, newversion, slot_size, buf, size);
    unlock_slot(newaddr);
   
    text(log_fp, "\t\t[ReaderWriter] We wrote  %" PRIu64 "\n", (*static_cast<uint64_t*>(buf)) );
    if(cb)    cb(newaddr,  1 , newversion, owner ); 
    return;
}

addr_t ReaderWriter::FixOneSlot(addr_t current_addr,uint16_t obj_id, addr_t new_base_addr){
    text(log_fp, "\t\t[FixOneSlot] FixOneSlot at addr %" PRIx64 ". We set this base  %" PRIx64 " \n",current_addr, new_base_addr );
 
    uint64_t shifted_newaddr = (new_base_addr >> BLOCK_BIT_SIZE);
 

    bool exhanged = true;
  
    std::atomic<slot_header_t> * head = (std::atomic<slot_header_t> *)current_addr;
    slot_header_t header_copy = head->load(std::memory_order_relaxed);
    if(header_copy.compaction || (header_copy.obj_id != obj_id)){
        text(log_fp, "\t\t[FixOneSlot] fix address under compaction   at %" PRIx64 " \n",   current_addr);
        return 0;
    }
    if(header_copy.oldbase == shifted_newaddr){
        // nothing to fix
        text(log_fp, "\t\t[FixOneSlot] address has been already fixed   at %" PRIx64 " \n",   current_addr);
        return 0; 
    }
    addr_t oldbase = header_copy.oldbase;
    slot_header_t fixed_header_copy = header_copy;
    fixed_header_copy.oldbase = shifted_newaddr; 
    exhanged = head->compare_exchange_strong(header_copy,fixed_header_copy);
    
    if(exhanged)
        return (oldbase << BLOCK_BIT_SIZE ) ;
    else
        return 0;
}


bool ReaderWriter::HasOldBaseSlot(addr_t old_base_addr, addr_t new_base_addr,uint16_t slot_size){
    text(log_fp, "\t\t[ReaderWriter] FixOneSlot in block %" PRIx64 ". We remove this address  %" PRIx64 " \n",new_base_addr, old_base_addr );
  
    uint64_t shifted_oldaddr = (old_base_addr >> BLOCK_BIT_SIZE);
    uint32_t total_objects = (BLOCK_USEFUL_SIZE) / slot_size;
    addr_t current_addr = new_base_addr;  

    for(uint32_t i=0; i < total_objects; i++ ){ 
        std::atomic<slot_header_t> * head = (std::atomic<slot_header_t> *)current_addr;
        slot_header_t header_copy = head->load(std::memory_order_relaxed);
        bool equal = (header_copy.oldbase == shifted_oldaddr) ;
        if(equal){
            return true;
        }
        current_addr+=slot_size;
    }
    return false;
}

void ReaderWriter::FixSlots(addr_t oldaddr, addr_t newaddr,uint16_t slot_size){
 
    uint64_t shifted_oldaddr = (oldaddr >> BLOCK_BIT_SIZE);
    uint64_t shifted_newaddr = (newaddr >> BLOCK_BIT_SIZE);
    
    uint32_t total_objects = (BLOCK_USEFUL_SIZE) / slot_size;

    addr_t current_addr = newaddr;  

    for(uint32_t i=0; i < total_objects; i++ ){ 
        bool equal = false;
        bool exhanged = true;
        do{
            std::atomic<slot_header_t> * head = (std::atomic<slot_header_t> *)current_addr;
            slot_header_t header_copy = head->load(std::memory_order_relaxed);
            equal = (header_copy.oldbase == shifted_oldaddr);
            if(equal){
                text(log_fp, "\t\t[ReaderWriter] Old base found for obj %u at %" PRIx64 " \n", i, current_addr);
                slot_header_t fixed_header_copy = header_copy;
                fixed_header_copy.oldbase = shifted_newaddr; 
                exhanged = head->compare_exchange_strong(header_copy,fixed_header_copy);
            }
        } while( equal && !exhanged);
        current_addr+=slot_size;
    }
 
}



addr_t ReaderWriter::ScanMemory(addr_t addr, uint16_t slot_size, uint16_t object_id ){
    text(log_fp,"\t\t[ScanMemory] Scan Block at  %" PRIx64 " of slot size %" PRIu16 " for object %" PRIu16 " \n ", addr, slot_size,object_id );
 
    uint32_t total_objects = (BLOCK_USEFUL_SIZE) / slot_size;
 
    addr_t current_addr = addr ; 
    for(uint32_t i=0; i < total_objects; i++ ){
  
        if( ((slot_header_t*)current_addr)->obj_id == object_id ){
            text(log_fp,"\t\t[ScanMemory] Object found  at %" PRIx64 " \n", current_addr);
            return current_addr;
        }
        current_addr+=slot_size;
    }
    text(log_fp,"\t\t[ScanMemory] Object has not been found block at %" PRIx64 " \n", addr);

    return 0;
}


int ReaderWriter::read_object_to_buffer(uint64_t to, uint64_t from, uint16_t obj_id, uint16_t slot_size, uint16_t *return_size, uint8_t *version ) {
    std::atomic<slot_header_t> * head = (std::atomic<slot_header_t> *)from;
    slot_header_t header_copy = head->load(std::memory_order_relaxed);
 
    if(header_copy.obj_id != obj_id){
        return NOT_FOUND;
    }

    if(header_copy.compaction){
        return UNDER_COMPACTION;
    }
    
    if(header_copy.lock){
        return LOCK_FAILED;
    }
 
    uint8_t current_version = header_copy.version;
    *version = current_version;
    text(log_fp, "\t\t[ReaderWriter] read current %" PRIx64 " \n", from);
    uint64_t next_cache_line = (from & CACHELINE_MASK) + CACHELINE; //
    text(log_fp, "\t\t[ReaderWriter] next_cache_line %" PRIx64 " \n", next_cache_line);
    uint64_t src = (uint64_t)from + sizeof(slot_header_t);
    uint64_t dst = (uint64_t)to; 
    uint64_t size = slot_size - sizeof(slot_header_t) ;

    assert(next_cache_line >= src && "Alignment is not correct! ");

    uint64_t copy_size = std::min(next_cache_line-src, size); 
    text(log_fp, "\t\t[ReaderWriter] can read copy  %" PRIu64 " \n", copy_size);
    memcpy((char*)dst, (char*)src, copy_size);
 
    uint64_t total_size = copy_size; 
    src  += copy_size;
    dst  += copy_size;
    size -= copy_size;

    // read over cachelines
    while(size > 0 ){
        text(log_fp, "\t\t[ReaderWriter] read version from %" PRIx64 " \n", src);
        if(current_version != *(uint8_t*)src){
            text(log_fp,"Error version, \n");
            return false;
        }
        src++;
        size--;
        copy_size = std::min((uint64_t)CACHELINE - 1, size); // 1 - is cache line version size
        text(log_fp, "\t\t[ReaderWriter] can read copy  %" PRIu64 " \n", copy_size);
        memcpy((char*)dst, (char*)src, copy_size);
        src  += copy_size;
        dst  += copy_size;
        size -= copy_size;
        total_size += copy_size;
    }  

    assert(src == from+slot_size && "Did you copy all bytes?"); 

    text(log_fp, "\t\t[ReaderWriter] read_locally finished \n");
    *return_size = (uint16_t) total_size;
    return 0;
}


int ReaderWriter::client_read_object_to_buffer_lim(uint64_t to, uint64_t from, 
    uint64_t actual_remote_address, uint16_t slot_size, uint16_t obj_id, uint32_t *lim_size ) {
    slot_header_t* h = (slot_header_t*)from;

    if(h->obj_id != obj_id){
        return NOT_FOUND;
    }

    if(h->lock){
        return LOCK_FAILED;
    }

    uint64_t actual_offset = actual_remote_address & mask_of_bits(BLOCK_BIT_SIZE);

    uint64_t upper_size = (*lim_size == 0) ? UINT64_MAX : *lim_size; 

    uint8_t current_version = h->version;
    text(log_fp, "\t\t[ReaderWriter] read current %" PRIx64 " \n", from);
    uint64_t next_cache_line = ((actual_offset + CACHELINE) &  CACHELINE_MASK) - actual_offset + from; //
    text(log_fp, "\t\t[ReaderWriter] next_cache_line %" PRIx64 " \n", next_cache_line);
    uint64_t src = (uint64_t)from + sizeof(slot_header_t);
    uint64_t dst = (uint64_t)to; 
    uint64_t size = slot_size - sizeof(slot_header_t);

    assert(next_cache_line >= src && "Alignment is not correct! ");

    uint64_t hop_size = std::min(next_cache_line-src, size); 

    uint64_t total_size = 0;
    uint64_t copy_size = std::min(hop_size, upper_size - total_size);
    memcpy((char*)dst, (char*)src, copy_size);

    total_size += copy_size; 
    src  += hop_size;
    dst  += copy_size;
    size -= hop_size;


    while(size > 0 ){
        text(log_fp, "\t\t[ReaderWriter] read version from %" PRIx64 " \n", src);
        if(current_version != *(uint8_t*)src){
            text(log_fp,"Error version, \n");
            return OBJECT_IS_INCONSISTENT;
        }
        src++;
        size--;
        hop_size = std::min((uint64_t)CACHELINE - 1, size);

        copy_size = std::min(hop_size, upper_size-total_size);
        memcpy((char*)dst, (char*)src, copy_size);
        total_size += copy_size; 
        src  += hop_size;
        dst  += copy_size;
        size -= hop_size;
    }  

    assert(src == from+slot_size && "Did you copy all bytes?"); 

    text(log_fp, "\t\t[ReaderWriter] read_locally finished \n");
    *lim_size = (uint32_t) total_size;
    return 0;
}


// for debugging 
int ReaderWriter::client_read_object_farm(uint64_t to, uint64_t from, 
    uint64_t actual_remote_address, uint16_t slot_size, uint32_t *lim_size ) {
    slot_header_t* h = (slot_header_t*)from;


    if(h->lock){
        return LOCK_FAILED;
    }

    uint64_t actual_offset = actual_remote_address & mask_of_bits(BLOCK_BIT_SIZE);

    uint64_t upper_size = (*lim_size == 0) ? UINT64_MAX : *lim_size; 

    uint8_t current_version = h->version;
    text(log_fp, "\t\t[ReaderWriter] read current %" PRIx64 " \n", from);
    uint64_t next_cache_line = ((actual_offset + CACHELINE) &  CACHELINE_MASK) - actual_offset + from; //
    text(log_fp, "\t\t[ReaderWriter] next_cache_line %" PRIx64 " \n", next_cache_line);
    uint64_t src = (uint64_t)from + sizeof(slot_header_t);
    uint64_t dst = (uint64_t)to; 
    uint64_t size = slot_size - sizeof(slot_header_t);

    assert(next_cache_line >= src && "Alignment is not correct! ");

    uint64_t hop_size = std::min(next_cache_line-src, size); 

    uint64_t total_size = 0;
    uint64_t copy_size = std::min(hop_size, upper_size - total_size);
    memcpy((char*)dst, (char*)src, copy_size);

    total_size += copy_size; 
    src  += hop_size;
    dst  += copy_size;
    size -= hop_size;


    while(size > 0 ){
        text(log_fp, "\t\t[ReaderWriter] read version from %" PRIx64 " \n", src);
        if(current_version != *(uint8_t*)src){
            text(log_fp,"Error version, \n");
            return OBJECT_IS_INCONSISTENT;
        }
        src++;
        size--;
        hop_size = std::min((uint64_t)CACHELINE - 1, size);

        copy_size = std::min(hop_size, upper_size-total_size);
        memcpy((char*)dst, (char*)src, copy_size);
        total_size += copy_size; 
        src  += hop_size;
        dst  += copy_size;
        size -= hop_size;
    }  

    assert(src == from+slot_size && "Did you copy all bytes?"); 

    text(log_fp, "\t\t[ReaderWriter] read_locally finished \n");
    *lim_size = (uint32_t) total_size;
    return 0;
}


int ReaderWriter::client_read_fast(uint64_t to, uint64_t from, uint16_t slot_size, uint32_t *lim_size ){
    uint32_t copy_size = std::min((uint32_t)slot_size, *lim_size );
    memcpy((char*)to, (char*)from, copy_size);
    return 0;
}

int ReaderWriter::try_lock_slot_and_increment_version(addr_t localbuf, uint16_t obj_id){ 
    std::atomic<slot_header_t> * head = (std::atomic<slot_header_t> *)localbuf;
    slot_header_t header_copy = head->load(std::memory_order_relaxed);
 
    if(header_copy.obj_id != obj_id){
        return NOT_FOUND;
    }

    if(header_copy.compaction){
        return UNDER_COMPACTION;
    }
 
    if(header_copy.lock){
        return LOCK_FAILED;
    }

    slot_header_t locked_header_copy = header_copy;
    locked_header_copy.lock = 1;
    locked_header_copy.version++;
    int version = (int)locked_header_copy.version;

    bool success = head->compare_exchange_strong(header_copy,locked_header_copy);
    if(success){
        return version;
    }
    return LOCK_FAILED;
}

int ReaderWriter::try_lock_slot_and_increment_version_if_version(addr_t localbuf, uint16_t obj_id, uint8_t requested_version){ 
    std::atomic<slot_header_t> * head = (std::atomic<slot_header_t> *)localbuf;
    slot_header_t header_copy = head->load(std::memory_order_relaxed);
    text(log_fp, "\t\t[ReaderWriter] %u == %u \n",(uint32_t)(header_copy.version), (uint32_t)requested_version);
    
    if(header_copy.obj_id != obj_id){
        return NOT_FOUND;
    }

    if(header_copy.compaction){
        return UNDER_COMPACTION;
    }

    if(header_copy.version != requested_version){
        return OBJECT_IS_INCONSISTENT;
    }

    if(header_copy.lock){
        return LOCK_FAILED;
    }
 
    slot_header_t locked_header_copy = header_copy;
    locked_header_copy.lock = 1;
    locked_header_copy.version++;
    int version = (int)locked_header_copy.version;

    bool success = head->compare_exchange_strong(header_copy,locked_header_copy);
    if(success){
        return version;
    }
    return -1;
}

bool ReaderWriter::try_lock_slot_for_compaction(addr_t localbuf){ 
    std::atomic<slot_header_t> * head = (std::atomic<slot_header_t> *)localbuf;
    slot_header_t header_copy = head->load(std::memory_order_relaxed);

    if(header_copy.lock){
        return false;
    }

    slot_header_t locked_header_copy = header_copy;
    locked_header_copy.lock = 1;
    locked_header_copy.compaction = 1;

    return head->compare_exchange_strong(header_copy,locked_header_copy);
}

void ReaderWriter::unlock_slot(addr_t localbuf){
    std::atomic<slot_header_t> * head = (std::atomic<slot_header_t> *)localbuf;
    slot_header_t header_copy = head->load(std::memory_order_relaxed);
    assert(header_copy.lock);

    slot_header_t unlocked_header_copy = header_copy;
    unlocked_header_copy.lock = 0;

    bool ret =  head->compare_exchange_strong(header_copy,unlocked_header_copy);
    assert(ret); 
}

bool ReaderWriter::unlock_slot_from_compaction(addr_t localbuf){
    std::atomic<slot_header_t> * head = (std::atomic<slot_header_t> *)localbuf;
    slot_header_t header_copy = head->load(std::memory_order_relaxed);
    assert(header_copy.lock);

    slot_header_t unlocked_header_copy = header_copy;
    unlocked_header_copy.lock = 0;
    unlocked_header_copy.compaction = 0;

    return head->compare_exchange_strong(header_copy,unlocked_header_copy);;
}
