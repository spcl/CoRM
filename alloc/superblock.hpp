/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * It is a super-block allocator. IT exists to use less fds to address physical memeory. 
 * So I can use one physical region for multiple blocks.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */

#pragma once
#include <iostream>
#include <string>
#include <cassert>
#include <unistd.h>
#include <sys/mman.h>
#include <linux/memfd.h>

#include <sys/syscall.h>
#ifndef MFD_HUGE_SHIFT  // for old linux kernels
     #include "memfd.h"
#endif
#include "block.hpp"
#include <bitset>

 
class SuperBlock{
    const uint32_t _id;
    int _fd; 
    std::bitset<BLOCKS_IN_SUPERBLOCK> _blocks; // 1 - is free, 0 - is allocated
    
public:
 
    SuperBlock(uint32_t id): _id(id) {
        text(log_fp,"Create Block name: %s\n",std::to_string(_id).c_str() );
 
        _fd = memfd_create( std::to_string(_id).c_str() , MFD_CLOEXEC );
        if (_fd == -1){
           exit(1);
        }

        int ret = ftruncate(_fd, BLOCKS_IN_SUPERBLOCK * BLOCK_SIZE);
        if (ret == -1){
          exit(1);
        } 
        // set all bits to true;
        _blocks.set();
        text(log_fp,"Success for superblock %s with file descriptor %d \n",std::to_string(_id).c_str(), _fd);
    }

 
    uint32_t getID() const{
        return _id;
    }
 
    int getFD() const{
        return _fd;
    }

    size_t getSize() const{
        return BLOCKS_IN_SUPERBLOCK * BLOCK_SIZE;
    }

    bool isFree() const{
        return _blocks.all();
    }

    bool isFull() const{
        return _blocks.none();
    }

    bool hasFreeBlocks() const{
        return _blocks.any();
    }

    Block* allocateBlock(){
        assert(hasFreeBlocks() && "SuperBlock has no free blocks");

        uint32_t offset_in_blocks = 0;
        for (uint32_t i = 0; i < _blocks.size(); ++i) {
 
            if(_blocks[i]){
                _blocks.reset(i); // set bit to 0
                offset_in_blocks = i;
                break;
            }
        }
        return new Block(_fd, offset_in_blocks);;
    }
 
    bool freeBlock(_block_phys_addr_t phys){
        assert(phys.fd == _fd && "attempt to deallocate to foreign block");
        _blocks.set(phys.offset_in_blocks); // set bit to 1
        return true;
    }
 
    ~SuperBlock(){
        text(log_fp,"\n========== DESTROY SUPERBLOCK[%u] ============ \n", _id);
        if(_fd!=-1){
            close(_fd);    
        }
    }

};

