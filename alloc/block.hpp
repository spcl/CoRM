/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * Block is a wraper over memfd descriptor that helps to create virtual addresses
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */
#pragma once
#include <sys/mman.h>
#include "../common/common.hpp"

 
class Block{
    const int fd;
    const uint32_t offset_in_blocks; 
public:
 
    Block(int fd, uint32_t offset_in_blocks): fd(fd), offset_in_blocks(offset_in_blocks){
        // nothing
    }
 
    uint32_t GetSize() const{
        return BLOCK_SIZE;
    }

    _block_phys_addr_t GetPhysAddr() const {
        return _block_phys_addr_t({fd,offset_in_blocks});
    }

    uint64_t CreateNewAddr() const{
        // Make address alligned to the size of the block
        if(BLOCK_SIZE > 4096 ){
            addr_t futurebuf =  (addr_t)(char*)mmap(NULL, 2*BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED| MAP_ANONYMOUS, -1, 0);
            addr_t alligned_addr = GetVirtBaseAddr(futurebuf);
            if(futurebuf - alligned_addr > 0){
                alligned_addr+=BLOCK_SIZE;
              //  printf(" %" PRIx64 " %" PRIx64 " " , futurebuf, ( alligned_addr - futurebuf ) );
                munmap((void*)futurebuf, ( alligned_addr - futurebuf ) ) ;
            }
            uint64_t extra = futurebuf + 2*BLOCK_SIZE - alligned_addr - BLOCK_SIZE;
            if(extra > 0){
             //   printf(" %" PRIx64 " %" PRIx64 " " , alligned_addr + BLOCK_SIZE, extra );
                munmap((void*)(alligned_addr + BLOCK_SIZE),  extra ) ;
            }
     
            char* res = (char*)mmap((void*)alligned_addr, BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset_in_blocks*BLOCK_SIZE);
            if (res == MAP_FAILED ){
              perror("mmap failed with NULL");
              exit(1);
            }
            return (uint64_t)alligned_addr;
        } else {
            char* alligned_addr = (char*)mmap(NULL, BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset_in_blocks*BLOCK_SIZE);
            if (alligned_addr == MAP_FAILED ){
              perror("mmap failed with NULL");
              exit(1);
            }
            return (uint64_t)alligned_addr;
        }
    }

    void RemapVirtAddrToMe(addr_t virt_addr) const {
        char* ret = (char*)mmap(ADDR_T_TO_PTR(virt_addr), BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED , fd, offset_in_blocks*BLOCK_SIZE);
        if (ret == MAP_FAILED){
           perror("mmap when is mapped to file with MAP_FIXED");
           exit(1);
        }
    }

    ~Block(){
        /* nothing */
    }

};
 
