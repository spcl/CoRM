/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A class for mapping a block to a thread allocator that owns that block.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */

#pragma once

#include "../alloc/alloc.hpp"
#include <atomic>
#include <stdlib.h>
#include "rcu.h"

class BlockHomeTable {
public:
    BlockHomeTable();
    ~BlockHomeTable();

    ThreadAlloc *Lookup(addr_t addr);
    void Insert(int thread_id, addr_t addr, ThreadAlloc *alloc);
    bool Update(addr_t addr, ThreadAlloc *alloc);
    void Remove(addr_t addr);

protected:
    struct KeyValuePair {
        addr_t addr;
        ThreadAlloc *alloc;
    };

    static const size_t BucketBytes = CACHELINE;
    static const size_t ItemsInBucket = BucketBytes / sizeof(KeyValuePair);
    
    static const size_t InitialBucketLog = 15;
    static const size_t InitialBucketCount = 1 << InitialBucketLog;

    struct Bucket {
        KeyValuePair items[ItemsInBucket];
    };

    static_assert(sizeof(Bucket) == BucketBytes, "Bucket of unexpected size");

    struct Header {
        size_t buckets;
        uint8_t pad[BucketBytes -
            sizeof(size_t) ];
    };

    static_assert(sizeof(Header) == BucketBytes, "Header of unexpected size");

    ThreadAlloc *LookupIn(Bucket *bucket_array, size_t bucket_count,
        uint64_t (*hash_fun)(addr_t addr), addr_t addr);
    ThreadAlloc *LookupIn(Bucket *bucket, addr_t addr);
 
    bool UpdateIn(ThreadAlloc *newalloc, Bucket *bucket, addr_t addr);


    bool InsertInto(Header *hdr, addr_t addr, ThreadAlloc *alloc);
    bool InsertInto(Bucket *bucket_array, size_t bucket_count,
        uint64_t (*hash_fun)(addr_t addr), addr_t addr, ThreadAlloc *alloc);
    bool InsertInto(Bucket *bucket, addr_t addr, ThreadAlloc *alloc);

    bool RemoveFrom(Bucket *array,  addr_t addr);
 
    void Grow(int thread_id);
    bool CopyItems(Header *new_hdr, Header *old_hdr);
    bool CopyBucket(Header *new_hdr, Bucket *bucket);

    static void AllFreeCb(void *owner);

    static void *AllocMemory(size_t size);
    static void FreeMemory(void *ptr);

protected:

    // a pointer to the table
    std::atomic<Header *> table;
};
  
// we use three hash functions for geting raget bucket in the hash table.
inline uint64_t hash_1(addr_t addr) {
    return addr >> BLOCK_BIT_SIZE;
}

inline uint64_t hash_2(addr_t addr) {
    uint64_t key = addr >> BLOCK_BIT_SIZE;
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdull;
    key ^= key >> 33;
    key *= 0xc4ceb9fe1a85ec53ull;
    key ^= key >> 33;
    return key;
}

inline uint64_t hash_3(addr_t addr) {
    uint64_t key = addr >> BLOCK_BIT_SIZE;
    key *= 0xc6a4a7935bd1e995ULL;
    key ^= key >> 47;
    key *= 0xc6a4a7935bd1e995ULL;
    return key;
}

inline uint64_t index(uint64_t hash, uint64_t buckets) {
    return hash & (buckets - 1);
}

BlockHomeTable::BlockHomeTable() {
    size_t size = InitialBucketCount * sizeof(Bucket) + sizeof(Header);
    Header *hdr = (Header *)AllocMemory(size);
    hdr->buckets = InitialBucketCount;
    table.store(hdr);
}

BlockHomeTable::~BlockHomeTable() {
    FreeMemory(table.load());
}

ThreadAlloc *BlockHomeTable::Lookup(addr_t addr) {
    ThreadAlloc *ret = nullptr;
    Header *hdr = table.load(std::memory_order_acquire);
    Bucket *bucket_array = (Bucket *)(hdr + 1);
    Bucket *bucket = nullptr;

    bucket = bucket_array + index(hash_1(addr), hdr->buckets);
    if((ret = LookupIn(bucket, addr)) != nullptr) {
        return ret;
    }
    bucket = bucket_array + index(hash_2(addr), hdr->buckets);
    if((ret = LookupIn(bucket, addr)) != nullptr) {
        return ret;
    }
    bucket = bucket_array + index(hash_3(addr), hdr->buckets);
    if((ret = LookupIn(bucket, addr)) != nullptr) {
        return ret;
    }
 
    // may return nullptr during compaction
    return nullptr;
}

ThreadAlloc *BlockHomeTable::LookupIn(Bucket *bucket, addr_t addr) {

    for(unsigned i = 0;i < ItemsInBucket;i++) {
        if(bucket->items[i].addr == addr) {
            std::atomic<addr_t> *addr_ptr = (std::atomic<addr_t> *)&bucket->items[i].addr;
            if(addr_ptr->load(std::memory_order_acquire) == addr) {
                return bucket->items[i].alloc;
            }
        }
    }

    return nullptr;
}


bool BlockHomeTable::Update(addr_t addr, ThreadAlloc *newalloc ) {
    Header *hdr = table.load(std::memory_order_acquire);
    Bucket *bucket_array = (Bucket *)(hdr + 1);
    Bucket *bucket =  nullptr;

    bucket =  bucket_array + index(hash_1(addr), hdr->buckets);
    if(UpdateIn(newalloc, bucket, addr)) {
        return true;
    }
    bucket =  bucket_array + index(hash_2(addr), hdr->buckets);
    if(UpdateIn(newalloc, bucket, addr)) {
        return true;
    }
    bucket =  bucket_array + index(hash_3(addr), hdr->buckets);
    if(UpdateIn(newalloc, bucket, addr)) {
        return true;
    }

    // may return nullptr during compaction
    return false;
}

bool BlockHomeTable::UpdateIn(ThreadAlloc *newalloc, Bucket *bucket, addr_t addr) {
 
    for(unsigned i = 0;i < ItemsInBucket;i++) {
        if(bucket->items[i].addr == addr) {
            std::atomic<addr_t> *addr_ptr = (std::atomic<addr_t> *)&bucket->items[i].addr;

            if(addr_ptr->load(std::memory_order_acquire) == addr) {
                bucket->items[i].alloc = newalloc;
                return true;
            }
        }
    }

    return false;
}



void BlockHomeTable::Insert(int thread_id, addr_t addr, ThreadAlloc *alloc) {
    while(!InsertInto(table.load(std::memory_order_relaxed), addr, alloc)) {
        printf("[BlockHomeTable] grow block table event\n");
        Grow(thread_id);
    }
}

bool BlockHomeTable::InsertInto(Header *hdr, addr_t addr, ThreadAlloc *alloc) {
    Bucket *bucket_array = (Bucket *)(hdr + 1);
    Bucket *bucket = nullptr;

    bucket = bucket_array + index(hash_1(addr), hdr->buckets);
    if(InsertInto(bucket, addr, alloc)) {
        return true;
    }
    bucket = bucket_array + index(hash_2(addr), hdr->buckets);
    if(InsertInto(bucket, addr, alloc)) {
        return true;
    }
    bucket = bucket_array + index(hash_3(addr), hdr->buckets);
    if(InsertInto(bucket, addr, alloc)) {
        return true;
    }

    return false;
}

bool BlockHomeTable::InsertInto(Bucket *bucket, addr_t addr, ThreadAlloc *alloc) {
    for(unsigned i = 0;i < ItemsInBucket;i++) {
        if(bucket->items[i].addr == 0) {
            bucket->items[i].alloc = alloc;
            std::atomic<addr_t> *addr_ptr = (std::atomic<addr_t> *)&bucket->items[i].addr;
            addr_ptr->store(addr, std::memory_order_release);
            return true;
        }

        assert(bucket->items[i].addr != addr);
    }

    return false;
}

void BlockHomeTable::Remove(addr_t addr) {
    Header *hdr = table.load(std::memory_order_relaxed);
    Bucket *bucket_array = (Bucket *)(hdr + 1);
    Bucket *bucket = nullptr;

    bucket = bucket_array + index(hash_1(addr), hdr->buckets);
    if(RemoveFrom(bucket, addr)) {
        return;
    }
    bucket = bucket_array + index(hash_2(addr), hdr->buckets);
    if(RemoveFrom(bucket, addr)) {
        return;
    }
    bucket = bucket_array + index(hash_3(addr), hdr->buckets);
    if(RemoveFrom(bucket, addr)) {
        return;
    }

    assert(0);
}

bool BlockHomeTable::RemoveFrom(Bucket *bucket, addr_t addr) {
    for(unsigned i = 0;i < ItemsInBucket;i++) {
        if(bucket->items[i].addr == addr) {
            std::atomic<addr_t> *addr_ptr = (std::atomic<addr_t> *)&bucket->items[i].addr;
            addr_ptr->store(0, std::memory_order_release);
            return true;
        }
    }

    return false;
}

void BlockHomeTable::Grow(int thread_id) {
    Header *hdr = table.load(std::memory_order_relaxed);
    size_t new_buckets = hdr->buckets;
    Header *new_hdr;

    while(true) {
        new_buckets <<= 1;
        size_t new_size = new_buckets * sizeof(Bucket) + sizeof(Header);

        new_hdr = (Header *)AllocMemory(new_size);
        new_hdr->buckets = new_buckets;

        if(CopyItems(new_hdr, hdr)) {
            break;
        }

        FreeMemory(new_hdr);
    }

    table.store(new_hdr, std::memory_order_release);

    BroadcastDrain::Drain(thread_id, AllFreeCb, hdr);    
}

bool BlockHomeTable::CopyItems(Header *new_hdr, Header *old_hdr) {
    size_t old_bucket_count = old_hdr->buckets;
    Bucket *old_buckets = (Bucket *)(old_hdr + 1);

    for(unsigned i = 0;i < old_bucket_count;i++) {
        if(!CopyBucket(new_hdr, old_buckets + i)) {
            return false;
        }
    }

    return true;
}

bool BlockHomeTable::CopyBucket(Header *new_hdr, Bucket *bucket) {
    for(unsigned i = 0;i < ItemsInBucket;i++) {
        ThreadAlloc *alloc = bucket->items[i].alloc;

        if(alloc != nullptr) {
            addr_t addr = bucket->items[i].addr;

            if(!InsertInto(new_hdr, addr, alloc)) {
                return false;
            }
        }
    }

    return true;
}

void BlockHomeTable::AllFreeCb(void *owner) {
    FreeMemory(owner);
}

void *BlockHomeTable::AllocMemory(size_t size) {
    void *ret = aligned_alloc(CACHELINE, size);
    memset(ret, 0, size);
    return ret;
}

void BlockHomeTable::FreeMemory(void *ptr) {
    free(ptr);
}

