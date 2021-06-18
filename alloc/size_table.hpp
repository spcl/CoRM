/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * It is a helper class to map user sizes to slot sizes. Note that I need to add cache verions and a header to each user's object.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */

#pragma once 


#include "../utilities/debug.h"
#include "../common/common.hpp"
 
 
class SizeTable{
public:
    // We want 256 classes so they can be indexed with uint8_t.
    static const uint32_t ClassCount = 64;
        // Align to word boundary.
public:
    static SizeTable& getInstance()
    {
        static SizeTable instance; // Guaranteed to be destroyed.
                              // Instantiated on first use.
        return instance;
    }

    SizeTable(SizeTable const&)               = delete;
    void operator=(SizeTable const&)  = delete;

    uint32_t objects_per_class[ClassCount]; // for compaction

private:
    static const uint32_t ClassAlignmentLog = 3;
    static const uint32_t ClassAlignment = (1 << ClassAlignmentLog);
 
    // The fist class is 64 bytes as we are unlikely to need more.
    static const uint32_t FirstClassSize = 8;

    static const uint32_t MaxAllowedFragmentation = 128;

    // Size of the lookup table.
    static const uint32_t LutBytes = 256;

    static const uint32_t MaxSizeInLut = LutBytes << ClassAlignmentLog;
 

    uint32_t size_map[ClassCount]; // real size
    uint8_t  lut[LutBytes];
    uint8_t last_lut_class;


    // private
    SizeTable(){

        uint32_t current_size = FirstClassSize;
        const uint32_t last_class_size = BLOCK_USEFUL_SIZE;

        // populate the array used for lookups
        for(uint32_t i = 0; i < ClassCount; i++) {
            // set the size
            uint32_t num_obj = last_class_size / current_size;
 
            while((current_size <=last_class_size) && (last_class_size % current_size > MaxAllowedFragmentation || num_obj == last_class_size / current_size)){
               current_size+=8;
            }

            num_obj = last_class_size / current_size;
            while((current_size <=last_class_size) && num_obj == last_class_size / current_size){
               current_size+=8;
            }
            current_size-=8;
 
            size_map[i] = current_size;

            objects_per_class[i] = (BLOCK_USEFUL_SIZE)/ current_size;
        }

        // initialize the lut
        for(uint32_t i = 0; i < LutBytes;i++) {
            uint32_t size = i << ClassAlignmentLog;
            lut[i] = (uint8_t)SearchClass(size, 0);
        }

        last_lut_class = lut[LutBytes - 1];
 
    };


public:

    void PrintTable(){
        for(uint32_t i=0; i < ClassCount; i++ ){
            info(log_fp,"Class[%u] -> real:%u user:%u;\n", i ,GetRealSize(i), GetUserSize(i));
        }
    }

    uint32_t SearchClass(uint32_t size, uint32_t start_idx) const {
        // Binary search classes with the caveat that we are not
        // looking for the exact match, but for the next larger size.
        uint32_t end_idx = ClassCount;

        while(start_idx < end_idx) {
            uint32_t mid_idx = (start_idx + end_idx) / 2;

            if(size_map[mid_idx] == size) {
                start_idx = end_idx = mid_idx;
            } else if(size_map[mid_idx] < size) {
                start_idx = mid_idx + 1;
            } else {
                end_idx = mid_idx;
            }
        }

        return end_idx;
    }
 
    
    uint8_t GetClassFromUserSize(uint32_t user_size) const{
        uint32_t real_size = user_size + sizeof(slot_header_t) + (user_size+sizeof(slot_header_t)-1)/CACHELINE  ;
        uint8_t class1 = GetClass(real_size);
        return GetUserSize(class1) >= user_size ? class1 : class1 + 1;
    };


    uint8_t GetClass(uint32_t real_size) const{
        if(real_size <= MaxSizeInLut) {
            return lut[(real_size + ClassAlignment - 1) >> ClassAlignmentLog];
        }

        return SearchClass(real_size, last_lut_class);
    };

    uint32_t GetRealSize(uint8_t size_class) const{
        return size_map[size_class];
    };

    uint32_t GetUserSize(uint8_t size_class) const{
        uint32_t real_size = size_map[size_class];
 
        if(real_size <= CACHELINE && CACHELINE % real_size == 0){
           return real_size - sizeof(slot_header_t); 
        } else {
           return real_size - sizeof(slot_header_t) - (real_size+CACHELINE-1)/CACHELINE;
        }

        
    };
  

};
