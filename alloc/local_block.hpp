/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * Implmenetation of a thread-local block. It helps to allocate addresses and manage metadata. 
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */
#pragma once

#include "block.hpp"
#include <forward_list>
#include <list>
#include <map>
#include <random>
#include <algorithm> 
#include <infiniband/verbs.h>


struct RandomGenerator{
 
    std::default_random_engine generator; 
    std::uniform_int_distribution<uint16_t> dis;  // not that I hard-coded only 2^16 ids. Change to have more

    RandomGenerator(uint32_t seed):generator(std::default_random_engine(seed)), dis(0,0xFFFF){ // not that I hard-coded only 2^16 ids. Change to have more

    }
    uint16_t GetNewRandomNumber(){  // not that I hard-coded only 2^16 ids. Change to have more
        return  dis(generator);
    }
};



struct LocalBlock
{
 
    LocalBlock(RandomGenerator &gen, Block* b, uint8_t type, uint16_t slot_size):
    _gen(gen), _b(b),  _size(_b->GetSize()),  _type(type),  _slot_size(slot_size), _slots( (BLOCK_USEFUL_SIZE) / slot_size)
    {
        _freeslots = _slots;
        _allocatedslots = 0;
        _obj_ids.clear();


        std::vector<offset_t> temp_vector_for_shuffle;
        temp_vector_for_shuffle.reserve(_slots);
        for(uint32_t slot_id= _slots-1; slot_id > 0; slot_id--){

            temp_vector_for_shuffle.push_back(slot_id*slot_size);
        }
        temp_vector_for_shuffle.push_back(0);

        std::shuffle ( temp_vector_for_shuffle.begin(), temp_vector_for_shuffle.end(), _gen.generator);

        _free_list = std::forward_list<offset_t>(temp_vector_for_shuffle.begin(), temp_vector_for_shuffle.end());
 

    }

    std::list< struct ibv_mr* >  all_virt_addr; 

    RandomGenerator &_gen;
    Block * const _b;
    const uint16_t  _size; 
    const uint8_t   _type;  
    const uint16_t  _slot_size;  
    const uint16_t  _slots; 

    uint16_t  _allocatedslots;
    uint16_t  _freeslots; 
    
    std::forward_list<offset_t> _free_list;
    // it stores offsets
    std::map<uint16_t, offset_t> _obj_ids;  // obj_id to offset

    uint32_t hasObjects( ) const {
        return _obj_ids.size();
    }

    void RemapVirtAddrToMe(addr_t addr) const {
        _b->RemapVirtAddrToMe(addr);
    }

    void AddNewVirtAddr(struct ibv_mr* mr){
        if(all_virt_addr.empty()){
            mr->lkey = 0;  // I reuse lkey as allocated counted
        }
        all_virt_addr.push_back(mr);
    }

    void GetAllAddrs(std::forward_list<addr_t> *list){
        for(auto &mr: all_virt_addr){
            list->push_front((addr_t)mr->addr);
        }
    }

    struct ibv_mr* RemoveVirtAddr(addr_t addr){
        auto it = std::find_if(all_virt_addr.begin(), all_virt_addr.end(),  [&addr] (struct ibv_mr* mr) { return (uint64_t)mr->addr == addr; });
        struct ibv_mr* mr = *it;
        all_virt_addr.erase(it);
        return mr;
    }

    struct ibv_mr* PopVirtAddr(){
        if(all_virt_addr.empty()){
            return NULL;
        }

        struct ibv_mr* mr = all_virt_addr.front();
        all_virt_addr.pop_front();
        return mr;
    }

    addr_t GetBaseAddr() const{
        assert(!all_virt_addr.empty());
        return  (addr_t)(all_virt_addr.front()->addr);
    }

    uint32_t GetRKey() const{
        assert(!all_virt_addr.empty());
        return  (all_virt_addr.front()->rkey);
    }

    _block_phys_addr_t GetPhysAddr() const{
        return  _b->GetPhysAddr();
    }

    uint8_t  GetType() const{
        return  _type;
    }

    uint16_t GetSlotSize() const{
        return _slot_size;
    }

    offset_t AllocSlot( ){
        offset_t offset = _free_list.front();
        _free_list.pop_front();
        _freeslots--;
        _allocatedslots++;

        all_virt_addr.front()->lkey++; // use to count objects in this virtaddr
        
        return offset;    
    }


    offset_t AllocObject(uint16_t *obj_id){
        text(log_fp, "\t\t\t[LocalBlock] AllocObject   \n");

        offset_t offset = AllocSlot();

        // find free id 
        uint16_t number = (_gen.GetNewRandomNumber() & mask_of_bits(ID_SIZE_BITS)); // bits ID_SIZE_BITS 
        auto it = _obj_ids.find (number);
        
        while ( it != _obj_ids.end() ){
            number = _gen.GetNewRandomNumber() & mask_of_bits(ID_SIZE_BITS);
            it = _obj_ids.find (number);
        }

        text(log_fp, "\t\t\t[LocalBlock] Assigned obj_id = %" PRIu16 " \n", number);
             
        _obj_ids.insert({number, offset});
        *obj_id = number;
 
        return offset;
    }

    offset_t  FindObject( uint16_t obj_id){
        text(log_fp, "\t\t\t[LocalBlock] Find obj_id = %" PRIu16 " \n", obj_id);

        auto it = _obj_ids.find (obj_id);
        assert(it != _obj_ids.end());

        return it->second;
    }


    offset_t  RemoveObject(uint16_t obj_id){

        text(log_fp, "\t\t\t[LocalBlock] Remove obj_id = %" PRIu16 "   \n", obj_id);

        auto it = _obj_ids.find(obj_id);

        if(it == _obj_ids.end()){
            info(log_fp, "\t\t\t[LocalBlock] obj_id = %" PRIu16 " does not exist \n", obj_id);
            return std::numeric_limits<offset_t>::max();
        }

        offset_t offset = it->second;
 
        _obj_ids.erase(it);
        _free_list.push_front(offset);

        _freeslots++;
        _allocatedslots--;
        return offset;
    }

    bool is_full() const{
        return _freeslots == 0;
    }        

    bool RemoveOneAddr(addr_t old_addr){
        auto it = std::find_if(all_virt_addr.begin(), all_virt_addr.end(),  [&old_addr] (struct ibv_mr* mr) { return (uint64_t)mr->addr == old_addr; });
        struct ibv_mr* mr = *it;
        mr->lkey--;
        bool can_be_unmapped = false;

        // if mr is not the main one and we deallocated all objects
        if(all_virt_addr.front() != mr && mr->lkey==0 ){
            can_be_unmapped = true;
        }
        return can_be_unmapped;
    }

    bool Compactible(LocalBlock* from){

        if( from->_type != this->_type ||  this->_freeslots  < from->_allocatedslots){
            return false;
        }

        // it assumes that both maps are sorted. Use merge sort to find intersections
        auto A_it = this->_obj_ids.begin();
        auto B_it = from->_obj_ids.begin();
        for (; A_it != this->_obj_ids.end() && B_it != from->_obj_ids.end(); )
        {
            if(A_it->first == B_it->first){
                return false;
            }

            if(A_it->first < B_it->first)
            {
                A_it++;
            }else{
                B_it++;
            }
        }
       
        return true;
    }



    void AddEntriesFrom(LocalBlock* from)
    {
        text(log_fp, "\t\t\t[LocalBlock] Moving  data from one block to another  \n");

        addr_t from_base = from->GetBaseAddr();
        addr_t to_base = this->GetBaseAddr();

        std::set<offset_t>   set_free_list (_free_list.cbegin(),_free_list.cend() );


        // here we try to place objects at the same offsets
        for (auto it = from->_obj_ids.cbegin(); it!=from->_obj_ids.cend(); /* no increment */)
        {
        
          auto sit = set_free_list.find(it->second);
          if ( sit != set_free_list.end() )
          {
                
            offset_t offset = it->second; 
            this->_obj_ids.insert({it->first, offset});
            text(log_fp, "\t\t\t[LocalBlock] Moved object to the same offset  %" PRIx32 " \n", offset);

            while(!ReaderWriter::try_lock_slot_for_compaction(from_base+offset)) { /* empty */};
            memcpy((void*)(to_base+offset), (void*)(from_base+offset), _slot_size );
            while(!ReaderWriter::unlock_slot_from_compaction(to_base+offset)) {/* empty */ };

            this->_freeslots--;
            this->_allocatedslots++;
            from->_obj_ids.erase(it++); 
            set_free_list.erase(sit);
          }
          else
          { 
            ++it;
          }
        }

        _free_list.clear();

        if(!set_free_list.empty()){
            std::vector<offset_t> temp_vector_for_shuffle(set_free_list.begin(), set_free_list.end());
            std::shuffle ( temp_vector_for_shuffle.begin(), temp_vector_for_shuffle.end(), _gen.generator);
            _free_list = std::forward_list<offset_t>(temp_vector_for_shuffle.begin(), temp_vector_for_shuffle.end());
        }  

        // here we try to place blocks at other offsets
        for(auto it = from->_obj_ids.begin(); it!=from->_obj_ids.end(); it++ ){

            addr_t offset = AllocSlot();
            text(log_fp, "\t\t\t[LocalBlock] Moved object to a new offset  %" PRIx64 " \n", offset);
            this->_obj_ids.insert({it->first, offset});
 
            while(!ReaderWriter::try_lock_slot_for_compaction(from_base+it->second)) { /* empty */};
            memcpy((void*)(to_base+offset), (void*)(from_base+it->second), _slot_size );
            while(!ReaderWriter::unlock_slot_from_compaction(to_base+offset)) {/* empty */ };
        }
    }


};
// helps to sort classes in c++
struct LocalBlockComp
{
  using is_transparent = void; 
  bool operator()(const LocalBlock* lhs, const LocalBlock* rhs) const { 
    if(lhs->_freeslots == rhs->_freeslots){
        return lhs < rhs;
    } else {
        return lhs->_freeslots < rhs->_freeslots;
    }
  }


  bool operator() (const LocalBlock* lhs, const uint32_t val) const
  {
      return lhs->_freeslots < val;
  }
};
