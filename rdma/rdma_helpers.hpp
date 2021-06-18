/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A simple manager of fixed-size regions used for send-receive communication.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */

#pragma once

#include <infiniband/verbs.h>
#include <set>

// it allocates buffers of fixed size
struct ReceiveBuffers{

    uint32_t const num_of_buffers;
    uint64_t const buffer_size;
    
    char *buffer;
    size_t const total_size;
    uint32_t lkey;
    struct ibv_mr * mr;

    ReceiveBuffers(uint32_t num_of_buffers, uint32_t buffer_size, struct ibv_pd *pd):
    num_of_buffers(num_of_buffers), buffer_size(buffer_size), total_size(num_of_buffers*buffer_size)
    {
        this->buffer = (char*)aligned_alloc(64, total_size);
        assert(buffer != NULL && "error memory allocation");

        this->mr = ibv_reg_mr(pd, buffer, total_size, IBV_ACCESS_LOCAL_WRITE);
        this->lkey = mr->lkey; 
    }

    char* get_buffer(uint32_t i) const {
        return buffer + i*buffer_size;
    }

    uint32_t get_lkey() const {
        return lkey;
    }

    uint32_t get_buffer_length() const {
        return buffer_size;
    }
    
    ~ReceiveBuffers(){
        if(mr)
            ibv_dereg_mr(mr);
        if(buffer)
            free(buffer);
    }

};

// it allocates buffers of any size.
// but it assumes short lifetime of objects.
struct SendBuffers{
    char *buffer;
    size_t const total_size;
    uint32_t lkey;
    struct ibv_mr * mr;

    uint32_t current_offset;
    uint32_t first_notfree;
  
    std::set<uint32_t> allocated_offsets;
 
    const uint32_t Allignment_bits = 6;
    const uint32_t Allignment = 1<<Allignment_bits;
    const uint32_t Allignment_mask = mask_of_bits(Allignment_bits);

    SendBuffers(size_t total_size, struct ibv_pd *pd):
    total_size(total_size), current_offset(0), first_notfree(total_size)
    {
        this->buffer = (char*)aligned_alloc(Allignment, total_size);
        assert(buffer != NULL && "error memory allocation");

        this->mr = ibv_reg_mr(pd, buffer, total_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
        if(mr==NULL){
            printf("Error reg mr\n");
        }
        this->lkey = mr->lkey; 
    }

    uint32_t get_lkey() const {
        return lkey;
    }

    char* Alloc(uint32_t size) {

        uint32_t to_allocate = (size + Allignment - 1) & ~Allignment_mask;

        if(current_offset+to_allocate >= total_size){
        // wrap around
            current_offset = 0;
            if(allocated_offsets.empty()){
                first_notfree = total_size;
            }else{
                first_notfree = *(allocated_offsets.begin());
            }
        }

        if(first_notfree-current_offset < to_allocate){
            // don't have memory
            printf("return don't have memory %u, total mem%lu\n",size,  total_size);
            return NULL;
        }

        size_t return_offset = current_offset;
        current_offset+=to_allocate;
        allocated_offsets.insert(return_offset);
        return buffer + return_offset;
    }

    void Free(char* buf) {
        uint32_t offset = (uint32_t)(buf - buffer);
        auto it = allocated_offsets.find(offset);
        assert(it != allocated_offsets.end() && "address does not exist");
        if(first_notfree == offset){
            auto next = std::next(it);
            if(next == allocated_offsets.end()){
                first_notfree = total_size;
            }else{
                first_notfree = *next;
            }
        }
        allocated_offsets.erase(it);
    }

    ~SendBuffers(){
        if(mr)
            ibv_dereg_mr(mr);
        if(buffer)
            free(buffer);       
    }
};
