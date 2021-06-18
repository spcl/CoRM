/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * An API of a client.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */


#pragma once 

#include <fcntl.h>
#include "communication.hpp"

#include "../rdma/connectRDMA.hpp"
#include "../rdma/rdma_helpers.hpp"
#include "../alloc/size_table.hpp"
#include "ReaderWriter.hpp"


struct LocalObjectHandler{
public:
    const uint32_t requested_size;
    const uint32_t allocated_size;   
    int version;
    client_addr_t addr;

    LocalObjectHandler(uint32_t requested_size, uint32_t allocated_size, client_addr_t addr, uint8_t version):
    requested_size(requested_size), allocated_size(allocated_size), version(version)
    {
        this->addr.parts[0] = addr.parts[0];
        this->addr.parts[1] = addr.parts[1];
    } 

    void print(){
        info(log_fp,"[Object]: requested size: %u allocated size: %u\n", requested_size, allocated_size);
        info(log_fp," \t\tAddr: %lu \n", addr.parts[0]);
        info(log_fp," \t\tAddr: %lu \n", addr.parts[1]);
        info(log_fp," \t\tversion: %u \n", version);
        info(log_fp," \t\tRkey: %lu \n", addr.comp.rkey);
    }
};


class RemoteMemoryClient{
    uint32_t thread_id;
    uint32_t req_id;
    VerbsEP* ep;

    // send buffers for each thread
    SendBuffers send_buffers;
    ReceiveBuffers recv_buffers;
 
    // work completion for each thread 
    struct ibv_wc completed_wc;
 
public:

    RemoteMemoryClient(uint32_t thread_id, VerbsEP* ep)
    : thread_id(thread_id), req_id(0), ep(ep), send_buffers(1024*1024, ep->pd), recv_buffers(5,4096,ep->pd)
    {
        for(uint32_t i=0; i<5; i++){
            post_recv(i);
        }
    }

    /* memory management */

    LocalObjectHandler* Alloc(uint32_t size);

    int Free(LocalObjectHandler* obj);

    int Write(LocalObjectHandler* obj, char* buffer, uint32_t size, bool atomic = false);

    int ReadOneSided(LocalObjectHandler* obj,  char* buffer, uint32_t length);

    int Read(LocalObjectHandler* obj,  char* buffer, uint32_t length);

    int FixPointer(LocalObjectHandler* obj); 




// For Benchmarking:
    int ReadOneSidedFix(LocalObjectHandler* obj,  char* buffer, uint32_t length);
    int RpcFake(char* buffer, uint32_t buf_length);
    int RpcFakeMesh(LocalObjectHandler* obj, char* buffer, uint32_t buf_length);
    int ReadOneSidedFake(LocalObjectHandler* obj,  char* buffer, uint32_t buf_length);
    int ReadOneSidedFarm(LocalObjectHandler* obj,  char* buffer, uint32_t buf_length);
    
    int TriggerCompaction(uint8_t size_class, bool with_collection, bool with_compaction);

private:
    void poll_send();
    void poll_reply();
    void inline post_recv(uint64_t wr_id);
};
 
  
void RemoteMemoryClient::post_recv(uint64_t wr_id){

    struct ibv_sge sge;
    struct ibv_recv_wr wr; 
    struct ibv_recv_wr *badwr;

    sge.addr = (uint64_t)recv_buffers.get_buffer(wr_id);
    sge.length = recv_buffers.get_buffer_length();
    sge.lkey = recv_buffers.get_lkey();

    wr.wr_id = wr_id;     
    wr.next = NULL;       
    wr.sg_list = &sge;    
    wr.num_sge = 1;    

    int ret = ibv_post_recv(ep->qp , &wr, &badwr);
    assert(ret==0 && "post recv failed");
}

void RemoteMemoryClient::poll_send(){
   
    while(ep->poll_send_completion(&completed_wc)==0);
    if(completed_wc.status!=IBV_WC_SUCCESS){
        info(log_fp,"\t[poll_send] Error send %d %u\n",completed_wc.status,completed_wc.vendor_err );
        if(completed_wc.status==IBV_WC_WR_FLUSH_ERR){
            info(log_fp,"\t[poll_send] We need to reconnect \n");
        }
    }


}

void RemoteMemoryClient::poll_reply(){
 
    while(ep->poll_recv_completion(&completed_wc)==0);
 
    // TODO process all states
    if(completed_wc.status!=IBV_WC_SUCCESS){
        info(log_fp,"\t[poll_reply] Error recv \n");
    }

    if(completed_wc.byte_len == 0){
        info(log_fp,"\t[poll_reply] empty \n");
        post_recv(completed_wc.wr_id);
    }

    text(log_fp,"\t[poll_reply] get reply\n");
};

// trigger compaction is async. The compaction will measured by the server; 
int RemoteMemoryClient::TriggerCompaction(uint8_t size_class, bool with_collection, bool with_compaction){
    uint32_t currentreq_id = (req_id++);
    text(log_fp,"\t[TriggerCompaction] try trigger compaction with request id %u \n", currentreq_id);
    {
        uint32_t length = sizeof(message_header_t) + sizeof(request_t);
        char* buf = send_buffers.Alloc(length);

        message_header_t* h = (message_header_t*)( buf );
        request_t* req = (request_t*)( buf + sizeof(message_header_t) );

        h->thread_id = thread_id;
        h->type = 1;

        req->type = RequestType::COMPACT;
        uint8_t reqtype = (with_collection? 2 : 0 ) +  (with_compaction? 1 : 0 );
        req->version = reqtype; // I reuse this field for type collection flag
        req->size = size_class;                 // I reuse this field for size class
        req->req_id = currentreq_id;
        req->addr.parts[0] = 0;
        req->addr.parts[1] = 0;
 
        ep->send_signaled((uint64_t)buf, (uint64_t)buf, send_buffers.get_lkey(), length);

        text(log_fp,"\t[TriggerCompaction] Request sent %u\n",length);
    }   

    poll_send();
    {
        char *buf = (char*)completed_wc.wr_id;
        send_buffers.Free(buf);
    }
    return 0;
}

/* memory management */
LocalObjectHandler* RemoteMemoryClient::Alloc(uint32_t size){
    //send request;
    uint32_t currentreq_id = (req_id++);
    text(log_fp,"\t[Alloc] try alloc with request id %u \n", currentreq_id);
    {
        uint32_t length = sizeof(message_header_t) + sizeof(request_t);
        char* buf = send_buffers.Alloc(length);

        message_header_t* h = (message_header_t*)( buf );
        request_t* req = (request_t*)( buf + sizeof(message_header_t) );

        h->thread_id = thread_id;
        h->type = 1;

        req->type = RequestType::ALLOC;
        req->version = 0;
        req->size = size;
        req->req_id = currentreq_id;
        req->addr.parts[0] = 0;
        req->addr.parts[1] = 0;
 
        ep->send_signaled((uint64_t)buf, (uint64_t)buf, send_buffers.get_lkey(), length);

        text(log_fp,"\t[Alloc] Request sent %u\n",length);
    }   

    poll_send();
    {
        char *buf = (char*)completed_wc.wr_id;
        send_buffers.Free(buf);
    }

    poll_reply();
 
    LocalObjectHandler* obj = NULL;
            // process here
    {
        char* buf = recv_buffers.get_buffer(completed_wc.wr_id); 
        //message_header_t* h = (message_header_t*)( buf );
        reply_t* rep = (reply_t*)( buf + sizeof(message_header_t) );

        assert(currentreq_id == rep->id);
        text(log_fp,"\t[Alloc] success at address %" PRIx64 "\n", rep->ret_addr.comp.addr);
        text(log_fp,"\t[Alloc] other metadata %" PRIx64 "\n", rep->ret_addr.parts[0]);
        // process here
        uint32_t allocated = SizeTable::getInstance().GetUserSize(rep->ret_addr.comp.type);
        obj = new LocalObjectHandler(size, allocated , rep->ret_addr, 0);
    }
    // done processing
    post_recv(completed_wc.wr_id);
    text(log_fp,"\t[Alloc] return %p\n", obj);
    return obj;
}



int RemoteMemoryClient::Free(LocalObjectHandler* obj){
    int ret = 0;
    text(log_fp,"\t[Free] Request %p\n",obj);
    uint32_t currentreq_id = (req_id++);
    text(log_fp,"\t[Free] try Free with request id %u \n", currentreq_id);
    {
        uint32_t length = sizeof(message_header_t) + sizeof(request_t);
        char* buf = send_buffers.Alloc(length);
        assert(buf!=NULL);

        message_header_t* h = (message_header_t*)( buf );
        request_t* req = (request_t*)( buf + sizeof(message_header_t) );

        h->thread_id = thread_id;
        h->type = 1;

        req->type = RequestType::FREE;
        req->version = 0;
        req->size = 0;
        req->req_id = currentreq_id;
        req->addr.parts[0] = obj->addr.parts[0]; 
        req->addr.parts[1] = obj->addr.parts[1]; 

        ep->send_signaled((uint64_t)buf, (uint64_t)buf, send_buffers.get_lkey(), length);
        text(log_fp,"\t[Free] Request sent %u\n",length);
    }

    poll_send();
    {
        char *buf = (char*)completed_wc.wr_id;
        send_buffers.Free(buf);
    }
    poll_reply();

    // process here
    {
        char* buf = recv_buffers.get_buffer(completed_wc.wr_id); 

        //message_header_t* h = (message_header_t*)( buf );
        reply_t* rep = (reply_t*)( buf + sizeof(message_header_t) );

        assert(currentreq_id == rep->id);
        if(rep->status != 0){
            text(log_fp,"\t[FREE] failed\n");
            ret = -1;
        }else{
            text(log_fp,"\t[FREE] success\n"); 
            if( obj->addr.comp.addr != rep->ret_addr.comp.addr){
                text(log_fp,"\t[FREE] fix pointer detected TODO\n");
                ret = 1;
            } 
            obj->version = -1;
        }
    }

    // done processing
    post_recv(completed_wc.wr_id);
 
    return ret; 
}


int RemoteMemoryClient::Write(LocalObjectHandler* obj, char* buffer, uint32_t size,  bool atomic){
    int ret = 0;
 
    if(atomic && obj->version < 0){ // read data first!
        return -1;
    }

    uint32_t currentreq_id = (req_id++);
    {
        uint32_t length = sizeof(message_header_t) + sizeof(request_t) + size;
        char* buf = send_buffers.Alloc(length);

        message_header_t* h = (message_header_t*)( buf );
        request_t* req = (request_t*)( buf + sizeof(message_header_t) );
        char* payload = (char*)(buf + sizeof(message_header_t) + sizeof(request_t));

        h->thread_id = thread_id;
        h->type = 1;
        if(atomic){
            req->type = RequestType::WRITEATOMIC;
            req->version= (uint8_t)obj->version;
        } else{
            req->type = RequestType::WRITE;  
            req->version = 0;
        }
        req->size = size;
        req->req_id = currentreq_id;
        req->addr.parts[0] = obj->addr.parts[0]; 
        req->addr.parts[1] = obj->addr.parts[1]; 
 
        memcpy( payload, buffer, size ); 
 
        text(log_fp,"\t[Write] Send version %" PRIu8 " and %u bytes\n", req->version, length );
        ep->send_signaled((uint64_t)buf, (uint64_t)buf, send_buffers.get_lkey(), length);
    }
    
    poll_send();
    {
        char *buf = (char*)completed_wc.wr_id;
        send_buffers.Free(buf);
    }
    poll_reply();
    // process here
    {
        char* buf = recv_buffers.get_buffer(completed_wc.wr_id); 

        //message_header_t* h = (message_header_t*)( buf );
        reply_t* rep = (reply_t*)( buf + sizeof(message_header_t) );

        assert(currentreq_id == rep->id);

        if(rep->status == 0){
            printf("failed read\n");
        }

        text(log_fp,"\t[Write] success %" PRIu8 "\n",rep->version );

        if( obj->addr.comp.addr != rep->ret_addr.comp.addr){
            // address changed. Need to perform fix pointer.
           ret = 1;
           // obj->fix_to_addr.addr = rep->ret_addr.addr;
           text(log_fp,"\t[Write] fix pointer detected\n");
        } 

        obj->version = rep->version;
    }
    // done processing
    post_recv(completed_wc.wr_id);
 
    return ret; 
}



int RemoteMemoryClient::Read(LocalObjectHandler* obj,  char* buffer, uint32_t buf_length){
    int ret = 0;
    

    uint32_t currentreq_id = (req_id++);

    {
        uint32_t length = sizeof(message_header_t) + sizeof(request_t);
        char* buf = send_buffers.Alloc(length);

        message_header_t* h = (message_header_t*)( buf );
        request_t* req = (request_t*)( buf + sizeof(message_header_t) );
 
        h->thread_id = thread_id;
        h->type = 1;

        req->type = RequestType::READ;
        req->version = 0;
        req->size = 0;
        req->req_id = currentreq_id;
        req->addr.parts[0] = obj->addr.parts[0]; 
        req->addr.parts[1] = obj->addr.parts[1]; 

        ep->send_signaled((uint64_t)buf, (uint64_t)buf, send_buffers.get_lkey(), length);
    }
    
    poll_send();
    {
        char *buf = (char*)completed_wc.wr_id;
        send_buffers.Free(buf);
    }
    poll_reply();

    // process here
    {
        char* buf = recv_buffers.get_buffer(completed_wc.wr_id); 

        //message_header_t* h = (message_header_t*)( buf );
        reply_t* rep = (reply_t*)( buf + sizeof(message_header_t) );
        char* payload = (char*)(buf + sizeof(message_header_t) + sizeof(reply_t));


        memcpy(buffer, payload, (buf_length<rep->data_size)? buf_length : rep->data_size); 
       // h->thread_id = thread_id;
       // h->type = 1;

        assert(currentreq_id == rep->id);

        if(rep->status==0){
            text(log_fp,"\t[Read] success %u\n",rep->status);
       
            if( obj->addr.comp.addr != rep->ret_addr.comp.addr){
                // address changed. Need to perform fix pointer.
               text(log_fp,"\t[Read] fix pointer detected %" PRIx64 " %" PRIx64 "  \n", obj->addr.comp.addr, rep->ret_addr.comp.addr);

               ret = 1;
               obj->addr.comp.addr = GetVirtBaseAddr(obj->addr.comp.addr) + GetAddressOffset(rep->ret_addr.comp.addr);
               // obj->fix_to_addr.addr = rep->ret_addr.addr;
//               info(log_fp,"\t[Read] fix pointer detected %" PRIx64 " %" PRIx64 "  \n", obj->addr.comp.addr, rep->ret_addr.comp.addr);
            } 

            obj->version = rep->version;
 
            text(log_fp,"\tThread(%u) Read Finished %u \n", thread_id, currentreq_id);
        } else {
            text(log_fp,"\t[Read] failed\n");
            ret = -1;
        }
    }
    // done processing
    post_recv(completed_wc.wr_id);
 
    return ret; 
} 

int RemoteMemoryClient::FixPointer(LocalObjectHandler* obj){
    int ret = 0;
 
    uint32_t currentreq_id = (req_id++);

    {
        uint32_t length = sizeof(message_header_t) + sizeof(request_t);
        char* buf = send_buffers.Alloc(length);
        message_header_t* h = (message_header_t*)( buf );
        request_t* req = (request_t*)( buf + sizeof(message_header_t) );
 

        h->thread_id = thread_id;
        h->type = 1;

        req->type = RequestType::FIXPOINTER;
        req->version = 0;
        req->size = 0;
        req->req_id = currentreq_id;
        req->addr.parts[0] = obj->addr.parts[0]; 
        req->addr.parts[1] = obj->addr.parts[1]; 

        ep->send_signaled((uint64_t)buf, (uint64_t)buf, send_buffers.get_lkey(), length);
 
    }
    
    poll_send();
    {
        char *buf = (char*)completed_wc.wr_id;
        send_buffers.Free(buf);
    }
    poll_reply();
    // process here
    {
        char* buf = recv_buffers.get_buffer(completed_wc.wr_id); 

        //message_header_t* h = (message_header_t*)( buf );
        reply_t* rep = (reply_t*)( buf + sizeof(message_header_t) );
    
        assert(currentreq_id == rep->id);

        text(log_fp,"\t[FixPointer] success\n");

 
        obj->addr.parts[0] = rep->ret_addr.parts[0]; 
        obj->addr.parts[1] = rep->ret_addr.parts[1];

        // TODO process status_type
        //rep->status;
    }
    // done processing
    post_recv(completed_wc.wr_id);

    text(log_fp,"\t[FixPointer] finished\n");
    return ret; 
} 
 

int RemoteMemoryClient::ReadOneSided(LocalObjectHandler* obj,  char* buffer, uint32_t buf_length){
    int ret = 0;
   
    addr_t addr =  obj->addr.comp.addr;
    uint16_t obj_id = obj->addr.comp.obj_id;
    uint8_t slot_type = obj->addr.comp.type;
    uint32_t slot_size = SizeTable::getInstance().GetRealSize(slot_type);

    char* sendbuf = send_buffers.Alloc(slot_size);

        // use send buf as receive buffer
    text(log_fp,"\t[ReadOneSided] signalled from rkey %u\n",(uint32_t)obj->addr.comp.rkey);
    text(log_fp,"\t[ReadOneSided] read %u, from %lu with rkey  %u\n",slot_size, addr, (uint32_t)obj->addr.comp.rkey);
    ep->read_signaled((uint64_t) sendbuf, (uint64_t) sendbuf, send_buffers.get_lkey(), addr, obj->addr.comp.rkey, slot_size);
    poll_send();
    // process here
 
    ret = ReaderWriter::client_read_object_to_buffer_lim((uint64_t)buffer, (uint64_t)sendbuf, addr, slot_size, 
                                                  obj_id, &buf_length );
    send_buffers.Free(sendbuf);
    
    if(ret==0){
        text(log_fp,"\t[ReadOneSided] success %lu %u \n", addr, buf_length);
        obj->version = GetSlotVersion((uint64_t)sendbuf);
    } else {
        if(ret == NOT_FOUND){
            text(log_fp,"\t[ReadOneSided] %lu %u has failed as the object is not found\n", addr, buf_length);

        }else{
            text(log_fp,"\t[ReadOneSided] %lu %u has failed with error %d\n", addr, buf_length,ret);
        }
    }
 
    // done processing
    text(log_fp,"\t[ReadOneSided] finished\n");
    return ret; 

}

int RemoteMemoryClient::ReadOneSidedFarm(LocalObjectHandler* obj,  char* buffer, uint32_t buf_length){
    int ret = 0;
   
    addr_t addr =  obj->addr.comp.addr;
    //uint16_t obj_id = obj->addr.comp.obj_id;
    uint8_t slot_type = obj->addr.comp.type;
    uint32_t slot_size = SizeTable::getInstance().GetRealSize(slot_type);

    char* sendbuf = send_buffers.Alloc(slot_size);

        // use send buf as receive buffer
    text(log_fp,"\t[ReadOneSidedFarm] signalled from rkey %u\n",(uint32_t)obj->addr.comp.rkey);
    text(log_fp,"\t[ReadOneSidedFarm] read %u, from %lu with rkey  %u\n",slot_size, addr, (uint32_t)obj->addr.comp.rkey);
    ep->read_signaled((uint64_t) sendbuf, (uint64_t) sendbuf, send_buffers.get_lkey(), addr, obj->addr.comp.rkey, slot_size);
    poll_send();
    // process here
 
    ret = ReaderWriter::client_read_object_farm((uint64_t)buffer, (uint64_t)sendbuf, addr, slot_size, 
                                                  &buf_length );
    send_buffers.Free(sendbuf); 
    // done processing
    text(log_fp,"\t[ReadOneSidedFarm] finished\n");
    return ret; 

}


int RemoteMemoryClient::ReadOneSidedFix(LocalObjectHandler* obj,  char* buffer, uint32_t buf_length){
    int ret = 0;
   
    addr_t addr =  GetVirtBaseAddr(obj->addr.comp.addr);
    uint16_t obj_id = obj->addr.comp.obj_id;
    uint8_t slot_type = obj->addr.comp.type;
    uint32_t slot_size = SizeTable::getInstance().GetRealSize(slot_type);

    char* sendbuf = send_buffers.Alloc(BLOCK_SIZE);
    text(log_fp,"\t[ReadOneSidedFix] read %u, from %lu with rkey  %u\n",BLOCK_SIZE, addr, (uint32_t)obj->addr.comp.rkey);

        // use send buf as receive buffer
    text(log_fp,"\t[ReadOneSidedFix] Read signalled from rkey %u\n",(uint32_t)obj->addr.comp.rkey);
    ep->read_signaled((uint64_t) sendbuf, (uint64_t) sendbuf, send_buffers.get_lkey(), addr, obj->addr.comp.rkey, BLOCK_SIZE);
    poll_send();
    // process here
    addr_t newaddr_local = ReaderWriter::ScanMemory( (uint64_t)sendbuf,slot_size,obj_id);
    if(newaddr_local == 0){
        send_buffers.Free(sendbuf);
        return NOT_FOUND;
    }

    addr_t newaddr = addr + (newaddr_local - (uint64_t) sendbuf); 
    obj->addr.comp.addr = newaddr;
 
    ret = ReaderWriter::client_read_object_to_buffer_lim((uint64_t)buffer, newaddr_local, newaddr, slot_size, 
                                                  obj_id, &buf_length );
    send_buffers.Free(sendbuf);
    
    if(ret==0){
        text(log_fp,"\t[ReadOneSidedFix] success %lu %u \n", newaddr, buf_length);
        obj->version = GetSlotVersion((uint64_t)newaddr_local);
    } else {
        if(ret == NOT_FOUND){
            assert(0 && "\tImpossible as we just found the object.");
        }else{
            text(log_fp,"\t[ReadOneSidedFix] %lu %u has failed with error %d\n", addr, buf_length,ret);
        }
    }


    // done processing
    text(log_fp,"\t[ReadOneSidedFix] finished\n");
    return ret; 

}
 
 
int RemoteMemoryClient::ReadOneSidedFake(LocalObjectHandler* obj,  char* buffer, uint32_t buf_length){
    int ret = 0;
   
    addr_t addr =  obj->addr.comp.addr;
    uint8_t slot_type = obj->addr.comp.type;
    uint32_t slot_size = SizeTable::getInstance().GetRealSize(slot_type);

    char* sendbuf = send_buffers.buffer; //Alloc(slot_size);

        // use send buf as receive buffer
    text(log_fp,"\t[ReadOneSidedFake] signalled from rkey %u\n",(uint32_t)obj->addr.comp.rkey);
    text(log_fp,"\t[ReadOneSidedFake] read %u, from %lu with rkey  %u\n",slot_size, addr, (uint32_t)obj->addr.comp.rkey);
    ep->read_signaled((uint64_t) sendbuf, (uint64_t) sendbuf, send_buffers.get_lkey(), addr, obj->addr.comp.rkey, slot_size);
    poll_send();
    // process here
    //send_buffers.Free(sendbuf);
    // done processing
    text(log_fp,"\t[ReadOneSidedFake] finished\n");
    return ret; 
}



int RemoteMemoryClient::RpcFake(char* buffer, uint32_t size){
    int ret = 0;
 
    uint32_t length = sizeof(message_header_t) + sizeof(request_t) + size;
    char* buf = send_buffers.buffer;//Alloc(length);
    message_header_t* h = (message_header_t*)( buf );
    h->thread_id = thread_id;
    h->type = 1;

    text(log_fp,"\t[RpcFake] Send and %u bytes\n",  length );
    ep->send_with_imm_signaled((uint64_t)buf, 1, (uint64_t)buf, send_buffers.get_lkey(), length);
 
    poll_send();
  
    //send_buffers.Free(buf);
 
    poll_reply();
    // process here
    post_recv(completed_wc.wr_id);
    return ret;  
}


int RemoteMemoryClient::RpcFakeMesh(LocalObjectHandler* obj, char* buffer, uint32_t size){
    int ret = 0;
 
    uint32_t length = sizeof(message_header_t) + sizeof(request_t);
    char* buf = send_buffers.buffer;//Alloc(length);
    message_header_t* h = (message_header_t*)( buf );
    h->thread_id = thread_id;
    h->type = 1;

    text(log_fp,"\t[RpcFake] Send and %u bytes\n",  length );
    ep->send_with_imm_signaled((uint64_t)buf, size, (uint64_t)buf, send_buffers.get_lkey(), length);
 
    poll_send();
  
    //send_buffers.Free(buf);
 
    poll_reply();
    // process here
    post_recv(completed_wc.wr_id);
    return ret;  
}

