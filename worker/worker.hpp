/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * The worker of CORM that takes requests and process them.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */

#pragma once

#include "../thread/thread.hpp"

#include <vector>
#include <unordered_map>
#include "../alloc/alloc.hpp"
#include "./communication.hpp"

#include "./ReaderWriter.hpp"
#include "../common/common.hpp"

#include "generic_worker.hpp"
#include "../rdma/connectRDMA.hpp"

#include "thread/messenger.hpp"


#include "../alloc/alloc_adapter.hpp"
#include "../rdma/rdma_helpers.hpp"


class Worker: public GenericWorker
{

    struct connection_ctx
    {
        Worker *const _this;
        ServerRDMA *const server;
        connection_ctx(Worker *_this, ServerRDMA *server): _this(_this), server(server)
        {
            //empty
        }
    };

    struct qp_event_ctx
    {
        Worker *const _this;
        VerbsEP *const ep;
        qp_event_ctx(Worker *_this, VerbsEP *ep): _this(_this), ep(ep)
        {
            // empty
        }
    };

    const uint32_t _id;
    ThreadAlloc *alloc;


    // only for leader
    ServerRDMA *server;
    std::vector<int> connectionsPerWorker;


    struct ibv_pd *const pd;
    struct ibv_srq *srq = NULL;

    struct ibv_cq *send_cq = NULL;
    struct ibv_cq *recv_cq = NULL;

    ReceiveBuffers recv_buffers;
    SendBuffers    send_buffers;

    std::unordered_map<uint32_t, VerbsEP *> connections;

    struct
    {
        uint32_t alloc;
        uint32_t free;
        uint32_t read;
        uint32_t write;
        uint32_t fixpointer;
        uint32_t compact;
        uint32_t reschedule;
    } stats;

public:
    Worker(uint32_t id, ThreadAlloc *alloc, struct ibv_pd *pd,  uint32_t recv_buffers_num, uint32_t recv_buffer_size, uint32_t send_buffer_size):
        _id(id),  alloc(alloc), server(NULL), pd(pd), recv_buffers(recv_buffers_num, recv_buffer_size, pd), send_buffers(send_buffer_size, pd)
    {
        memset(&stats, 0, sizeof(stats)); // zero stats
        this->srq = ServerRDMA::create_srq(pd, recv_buffers_num);
        for(uint32_t i = 0; i < recv_buffers_num; i++)
        {
            post_recv_buf(i);
        }

        this->send_cq = ibv_create_cq(pd->context, recv_buffers_num*4, NULL, NULL, 0);
        this->recv_cq = ibv_create_cq(pd->context, recv_buffers_num*4, NULL, NULL, 0);

    };


    void  post_recv_buf(uint32_t wr_id)
    {
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

        int ret = ibv_post_srq_recv(this->srq, &wr, &badwr);
        assert(ret == 0 && "error at posing shared receives");
    }

    void print_stats()
    {
        this->alloc->print_stats();

        if(this->local_thread_id == 0){
              AllocAdapter::getInstance().print_stats();
        }

        info(log_fp, "[Worker(%d)] Stats; Alloc %u, Read %u, Write %u, Free %u\n", this->local_thread_id, stats.alloc, stats.read,  stats.write, stats.free );
    }

    void set_rdma_server(ServerRDMA *server)
    {
        this->server = server;
    }

    void set_thread_id(uint8_t id) override
    {
        this->local_thread_id = id;
        mts::SetWorker(local_thread_id, (void *)this);
    }

    uint8_t get_thread_id() const
    {
        return this->local_thread_id;
    }

    void set_io_watcher(IOWatcher *w) override
    {
        this->local_io_watcher = w;
        if(this->server != NULL)
        {
            connectionsPerWorker.resize(mts::num_threads);
            int con_fd = server->get_listen_fd();
            text(log_fp, "[Worker(0)] Install listener for RDMA connections %d \n", con_fd);
            connection_ctx *ctx = new connection_ctx(this, server);
            this->local_io_watcher->install_io(con_fd, &NewConnectionCb, ctx );
        }
    }

    static void NewConnectionCb( uint32_t fd, void *ctx )
    {
        connection_ctx *_ctx = (connection_ctx *)(ctx);
        _ctx->_this->NewConnection(fd, _ctx);

    }

    void NewConnection( uint32_t fd, connection_ctx *ctx )
    {

        text(log_fp, "[NewConnection(%u)] new connection request is detected \n", local_thread_id);
        struct rdma_cm_id *new_con = ctx->server->getConnectRequest();
        auto result = std::min_element(connectionsPerWorker.begin(), connectionsPerWorker.end());
        int target_id = std::distance(connectionsPerWorker.begin(), result);

        connectionsPerWorker[target_id]++;

        if( target_id != this->local_thread_id)
        {
            text(log_fp, "[NewConnection(%u)] new connection pushed to %d\n", local_thread_id, target_id);
            mts::thread_msg_t *msg = new mts::thread_msg_t();
            msg->cb = &Worker::NewConnectionMsgCb;
            msg->payload[0] = (void *)mts::GetWorker(target_id);
            msg->payload[1] = (void *)new_con;
            mts::send_msg_to_thread_and_notify(target_id, msg);
            return;
        }
        AcceptConnection(new_con);
    }

    static void NewConnectionMsgCb( mts::thread_msg_t *msg )
    {
        Worker *_this = (Worker *)msg->payload[0];
        struct rdma_cm_id *new_con = (struct rdma_cm_id *)msg->payload[1];
        _this->AcceptConnection(new_con);
        delete msg; // deallocated by another thread, but I think it is not critical here
    }

    void AcceptConnection(struct rdma_cm_id *new_con)
    {


        struct ibv_qp_init_attr attr;
        struct rdma_conn_param conn_param;
        memset(&attr, 0, sizeof(attr));
        attr.cap.max_send_wr = 32;
        attr.cap.max_recv_wr = 0; // since I use shared receive queue.
        attr.cap.max_send_sge = 1;
        attr.cap.max_recv_sge = 1;
        attr.cap.max_inline_data = 0;
        attr.qp_type = IBV_QPT_RC;
        attr.srq = this->srq;

        attr.send_cq = this->send_cq;
        attr.recv_cq = this->recv_cq;

        memset(&conn_param, 0, sizeof(conn_param));
        conn_param.responder_resources = 5;
        conn_param.initiator_depth = 0;
        conn_param.retry_count = 3;
        conn_param.rnr_retry_count = 3;


        VerbsEP *ep = ServerRDMA::acceptEP(new_con, &attr, &conn_param, this->pd);

        this->connections.insert({ep->get_qp_num(), ep});
        text(log_fp, "[NewConnection(%u)] new connection is accepted with qpn %u\n", local_thread_id, ep->get_qp_num());

        // install conection listener
        int con_fd = ep->get_event_fd();
        text(log_fp, "[NewConnection(%u)] Install listener for the connection %d \n", local_thread_id, con_fd);
        qp_event_ctx *ctx = new qp_event_ctx(this, ep);
        this->local_io_watcher->install_io(con_fd, &NewQpEventCb, ctx );
    }



    static void NewQpEventCb( uint32_t io_id, void *ctx )
    {
        qp_event_ctx *_ctx = (qp_event_ctx *)(ctx);
        _ctx->_this->ProcessQpEvent(io_id, _ctx);
    }

    void ProcessQpEvent(uint32_t io_id, qp_event_ctx *ctx )
    {
        text(log_fp, "[ProcessQpEvent(%u)] event for io_id %d \n", local_thread_id, io_id);
        // TODO check whether it is a disconnect event

        this->local_io_watcher->stop_io(io_id);
        VerbsEP *ep = ctx->ep;
        RemoveConnection(ep);
        delete ep; // TODO if we have delayed opertaions to disconnected EP, we get seg fault. We need to delay disconnect as well. 
        delete ctx;

        uint32_t serverid = 0;
        if(this->local_thread_id != serverid)
        {
            mts::thread_msg_t *msg = new mts::thread_msg_t();
            msg->cb = &Worker::DisconnectMsgCb;
            msg->payload[0] = (void *)mts::GetWorker(serverid);
            msg->payload[1] = (void *)(uint64_t)this->local_thread_id;
            mts::send_msg_to_thread_and_notify(serverid, msg);
        }
        else
        {
            Disconnect(this->local_thread_id);
        }

    }

    void RemoveConnection(VerbsEP *con)
    {
        connections.erase(con->get_qp_num());
        text(log_fp, "[Worker(%u)] A connection has been removed\n", local_thread_id);
    }

    void Disconnect(uint32_t target_id)
    {
        text(log_fp, "[Worker(%u)] Disconnect client \n", local_thread_id);
        connectionsPerWorker[target_id]++;
    }

    static void DisconnectMsgCb( mts::thread_msg_t *msg )
    {
        Worker *_this = (Worker *)msg->payload[0];
        uint32_t target_id = (uint32_t)(uint64_t)msg->payload[1];
        _this->Disconnect(target_id);
        delete msg;  // deallocated by another thread, but I think it is not critical here
    }

    void main_cb() override;

    void sometimes_cb() override;


    void inline PollRecvRequest();
    void inline PollSendRequest();


    struct AllocCtx
    {
        VerbsEP *ep;
        uint64_t wr_id;
        Worker *_this;
        uint32_t request_id;
        uint8_t client;
        thread_alloc_cb cb;
        AllocCtx(VerbsEP *ep, uint64_t wr_id, Worker *w, uint32_t id, uint8_t client, thread_alloc_cb cb ):
            ep(ep), wr_id(wr_id), _this(w), request_id(id), client(client), cb(cb)
        {
            // empty
        }
    };

    struct FreeCtx
    {
        VerbsEP *ep;
        uint64_t wr_id;
        Worker *_this;
        uint32_t request_id;
        client_addr_t addr;
        uint8_t client;
        thread_free_cb cb ;
        int retry;
        FreeCtx(VerbsEP *ep, uint64_t wr_id, Worker *w, uint32_t id, client_addr_t addr, uint8_t client, thread_free_cb cb ):
            ep(ep), wr_id(wr_id), _this(w), request_id(id), addr(addr), client(client), cb(cb), retry(0)
        {
            // empty
        }
    };

    void ProcessRequest(VerbsEP *ep, uint64_t wr_id);
    void ProcessFakeRequest(VerbsEP *ep, uint64_t wr_id, uint32_t immdata);
    void inline CompleteSend(uint64_t wr_id);

private:

    struct ReadWriteCtx
    {
        VerbsEP *ep;
        uint64_t wr_id;
        Worker *_this;
        uint32_t request_id;
        client_addr_t client_addr;
        uint8_t client;
        uint8_t type;
        uint32_t retry = 0;
        uint32_t size;
        uint8_t version;
        char *buffer;
        ReadWriteCtx(VerbsEP *ep, uint64_t wr_id, Worker *w, uint32_t id, client_addr_t client_addr, uint8_t client, uint8_t type,  uint32_t size = 0, uint8_t version = 0):
            ep(ep), wr_id(wr_id), _this(w), request_id(id), client_addr(client_addr), client(client), type(type), size(size), version(version), buffer(NULL)
        {
            // empty
        }
    };

    static void FindCbRet(addr_t newaddr,  uint16_t slot_size,  void *owner);
    void FindRet(addr_t newaddr,  uint16_t slot_size,  ReadWriteCtx *ctx);
    static void DelayedFindObjectAddrCb(int rev, void *arg);


    static void AllocCbRet(client_addr_t client_addr, void *owner);
    void AllocRet( client_addr_t ret_addr, AllocCtx *ctx);



    static void FreeCbRet( int status, addr_t newaddr, void *owner);
    void FreeRet(int status, addr_t newaddr,  FreeCtx *ctx);
    static void DelayedFreeAddrCb(int rev, void* owner);

    static void  ReadCbRet(uint16_t size, addr_t newaddr, uint8_t version, void *owner);
    void ReadRet(uint32_t size, addr_t newaddr, uint8_t version, ReadWriteCtx *ctx);


    static void WriteCbRet( addr_t newaddr, uint8_t success, uint8_t version, void *owner);
    void WriteRet(addr_t newaddr, uint8_t success, uint8_t version, ReadWriteCtx *ctx);

    struct FixPointerCtx
    {
        VerbsEP *ep;
        uint64_t wr_id;
        Worker *_this;
        uint32_t request_id;
        client_addr_t addr;
        uint8_t client;
        FixPointerCtx(VerbsEP *ep, uint64_t wr_id, Worker *w, uint32_t id, client_addr_t addr, uint8_t client ):
            ep(ep), wr_id(wr_id), _this(w), request_id(id), addr(addr), client(client)
        {
            // empty
        }
    };

    static void FixPointerCbRet( int ret, client_addr_t newaddr,  void *owner);
    void FixPointerRet(uint8_t success, client_addr_t newaddr, FixPointerCtx *ctx);

};


void Worker::main_cb()
{
    PollRecvRequest();
    PollSendRequest();
}


void Worker::sometimes_cb()
{
    info(log_fp, "[Worker(%d)] Stats; Alloc %u, Read %u, Write %u, Free %u\n", this->local_thread_id, stats.alloc, stats.read,  stats.write, stats.free );
}

/*
All threads poll all receive qp to get request from clients.
imlemented shared queues in spin. Here will be simpler, because of ibverbs
*/
void Worker::PollRecvRequest()
{
    struct ibv_wc wc;
    int ret = ibv_poll_cq(this->recv_cq, 1, &wc);
    if(ret > 0)
    {
        if(wc.status == IBV_WC_SUCCESS)
        {
            text(log_fp, "[Worker(%u)] Get request!\n\n", local_thread_id);
            if(wc.wc_flags & IBV_WC_WITH_IMM){\

                ProcessFakeRequest(connections[wc.qp_num], wc.wr_id, wc.imm_data);
            }
            else{
                ProcessRequest(connections[wc.qp_num], wc.wr_id);
            }
        }
        else
        {
            printf("Error on recv. TODO solve the issue %d\n",wc.status);
        }
    }
}

void Worker::PollSendRequest()
{
    struct ibv_wc wc;
    int ret = ibv_poll_cq(this->send_cq, 1, &wc);
    if(ret > 0)
    {
        if(wc.status == IBV_WC_SUCCESS)
        {
            text(log_fp, "[Worker(%u)] completed send!\n\n", local_thread_id);
            CompleteSend(wc.wr_id);
        }
        else
        {
            printf("Error %d on send. TODO solve the issue\n", wc.status);
        }
    }
}

void Worker::CompleteSend(uint64_t wr_id)
{   
    if(wr_id !=0){
        char *buf = (char *)wr_id;
        send_buffers.Free(buf);  
    }
}
void Worker::ProcessFakeRequest(VerbsEP *ep, uint64_t wr_id, uint32_t immdata)
{
    char *req_buffer = recv_buffers.get_buffer( (uint32_t)wr_id );
    message_header_t *orig_header = (message_header_t *)req_buffer;

    uint32_t reply_size = sizeof(message_header_t) + sizeof(reply_t);
    char *buffer = NULL;

    if(immdata > 10){
        reply_size+=immdata;
        char *tempbuffer = send_buffers.Alloc(reply_size);

        buffer = send_buffers.Alloc(reply_size);
        memcpy(buffer,tempbuffer,reply_size);
        send_buffers.Free(tempbuffer);  
    } else {
        buffer = send_buffers.Alloc(reply_size);
    }
 
    
    message_header_t *header = (message_header_t *)( buffer );
    header->thread_id = orig_header->thread_id;
    header->type = 0;


    ep->send_signaled((uint64_t)buffer, (uint64_t)buffer, send_buffers.get_lkey(), reply_size);
    post_recv_buf(wr_id);
}

void Worker::ProcessRequest(VerbsEP *ep, uint64_t wr_id)
{

    char *req_buffer = recv_buffers.get_buffer( (uint32_t)wr_id );

    message_header_t *header = (message_header_t *)req_buffer;
    request_t *req =  (request_t *) ((char *)req_buffer + sizeof(message_header_t) );

    //parse the header
    uint8_t client = header->thread_id;
    //parse the request
    uint8_t version = req->version;
    uint32_t req_id = req->req_id;
    client_addr_t client_addr = req->addr;
    uint32_t size = req->size;

    switch(req->type)
    {
    case (RequestType::READ) :
    {

        stats.read++;

        post_recv_buf(wr_id);

        text(log_fp, "[Worker(%u)] Thread(%u) Read request %u \n", local_thread_id, client, req_id);
        text(log_fp, "\t[Worker(%u)] Found read request to address %" PRIx64 "\n", local_thread_id, client_addr.comp.addr);
        ReadWriteCtx *ctx =  new ReadWriteCtx(ep, wr_id, this,
                                              req_id, client_addr, client, RequestType::READ);

        alloc->FindObjectAddr(client_addr, FindCbRet, ctx );
        break;
    }
    case (RequestType::WRITE) :
    {
        stats.write++;

        text(log_fp, "\t[Worker(%u)] Found write request to address %" PRIx64 "\n", local_thread_id, client_addr.comp.addr);
        ReadWriteCtx *ctx = new ReadWriteCtx(ep, wr_id, this,
                                             req_id, client_addr, client, RequestType::WRITE, size);
        alloc->FindObjectAddr(client_addr, FindCbRet, ctx );
        break;
    }
    case (RequestType::WRITEATOMIC):
    {
        stats.write++;
        text(log_fp, "\t[Worker(%u)] Found atomic write request to address %" PRIx64 " with version %u\n", local_thread_id, client_addr.comp.addr, version);
        ReadWriteCtx *ctx = new ReadWriteCtx(ep, wr_id, this,
                                             req_id, client_addr, client, RequestType::WRITEATOMIC, size, version);
        alloc->FindObjectAddr(client_addr, FindCbRet, ctx );

        break;
    }
    case (RequestType::ALLOC) :
    {
        stats.alloc++;

        post_recv_buf(wr_id);
        text(log_fp, "\t[Worker(%u)] Found alloc request to address of size %" PRIu32 "\n", local_thread_id, size);
        AllocCtx *ctx = new AllocCtx(ep, wr_id, this, req_id,  client, AllocCbRet);
        AllocAdapter::getInstance().Alloc(this->_id, size, ctx);
        //alloc->Alloc(size, AllocCbRet, ctx);
        break;
    }
    case (RequestType::FREE) :
    {
        stats.free++;
        post_recv_buf(wr_id);
        text(log_fp, "\t[Worker(%u)] Found free request to address %" PRIx64 "\n", local_thread_id, client_addr.comp.addr);
        FreeCtx *freectx = new FreeCtx(ep, wr_id, this, req_id, client_addr, client, FreeCbRet);
        AllocAdapter::getInstance().Free(this->_id, client_addr, freectx);
        //alloc->Free(client_addr, FreeCbRet, freectx );
        break;
    }
    case (RequestType::FIXPOINTER) :
    {
        stats.fixpointer++;

        post_recv_buf(wr_id);

        text(log_fp, "\t[Worker(%u)] Found fixpointer request to address %" PRIx64 "\n", local_thread_id, client_addr.comp.addr);
        FixPointerCtx *ctx = new FixPointerCtx(ep, wr_id, this, req_id, client_addr, client);
        // alloc will unmap virtaddr and returns a new address of the block
        alloc->FixClientAddr(client_addr, FixPointerCbRet, ctx );

        break;
    }
    case (RequestType::COMPACT) :
    {
        stats.compact++;

        post_recv_buf(wr_id);

        uint32_t class_to_compact = size; // I use the field for that. 
        // I trigger compaction and do not reply to client. 
        uint8_t reqtype = version; // do I need to collect first? 
        text(log_fp, "\t[Worker(%u)] RequestType::COMPACT  with reqtype %u \n", local_thread_id, reqtype);
        if(reqtype == 1){
            alloc->Compaction(class_to_compact); 
        }else if(reqtype == 2){
            AllocAdapter::getInstance().trigger_type_collection(class_to_compact, false);
        }else if(reqtype == 3){
            AllocAdapter::getInstance().trigger_type_collection(class_to_compact, true);
        }

        break;
    }
    default:
    {
        post_recv_buf(wr_id);
        text(log_fp, "\t[Worker(%u)] Unknown type request: %" PRIu8 "\n", local_thread_id, req->type);
    }
    }

    return;
}


void Worker::FindCbRet(addr_t newaddr,  uint16_t slot_size,  void *owner)
{
    ReadWriteCtx *ctx = (ReadWriteCtx *)owner;
    ctx->_this->FindRet( newaddr, slot_size, ctx);
}

void Worker::FindRet(addr_t newaddr,  uint16_t slot_size,  ReadWriteCtx *ctx)
{
 
    if(newaddr == 0)
    {   
        info(log_fp, "\t[Worker(%u)] Address: %" PRIx64 " has not been found\n", local_thread_id, newaddr);
        ctx->retry++;
        if(ctx->retry < 3){
            stats.reschedule++;
            info(log_fp, "\t[Worker(%u)] reschedule as the object was not found \n", local_thread_id);
            mts::SCHEDULE_CALLBACK(local_thread_id, 1.4, DelayedFindObjectAddrCb, ctx); // alloc->FindObjectAddr(client_addr, FindCbRet, ctx );
            return;
        } else{
            if (ctx->type == RequestType::READ ){
                ReadRet(0,0,0,ctx);
            } else {
                WriteRet(0, 1, 0, ctx);
            }
 
            return;
        }
    }
 
    text(log_fp, "\t\t[Worker(%u)] Found addr to request (%u) %" PRIu32 "\n", local_thread_id, ctx->client, ctx->request_id);
    if (ctx->type == RequestType::READ )
    {
        if(ctx->buffer == NULL){
            uint32_t replysize = slot_size + sizeof(message_header_t) + sizeof(reply_t);
            ctx->buffer = send_buffers.Alloc(replysize);;
        }

        ReaderWriter::ReadObjectToBuf(ctx->buffer + sizeof(message_header_t) + sizeof(reply_t),
                                            newaddr, GetObjId(ctx->client_addr),slot_size, ReadCbRet, ctx );
        return;
    }
    else if(ctx->type == RequestType::WRITE)
    {
        char *buffer = recv_buffers.get_buffer( (uint32_t)ctx->wr_id ) + sizeof(request_t) + sizeof(message_header_t);
        text(log_fp, "\t\t[ReaderWriter] We write  %" PRIu64 "\n", (*reinterpret_cast<uint64_t *>(buffer)) );

        ReaderWriter::WriteBufToObject(newaddr, GetObjId(ctx->client_addr), slot_size, buffer, ctx->size, WriteCbRet, ctx );
        return;
    }
    else
    {
        text(log_fp, "\t\t[Worker(%u)] WriteBufToObjectAtomic \n", local_thread_id);
        char *buffer = recv_buffers.get_buffer( (uint32_t)ctx->wr_id ) +  sizeof(request_t) + sizeof(message_header_t);
        ReaderWriter::WriteBufToObjectAtomic(newaddr, GetObjId(ctx->client_addr), slot_size, ctx->version, buffer, ctx->size, WriteCbRet, ctx );
        return;
    }
 
}


void Worker::DelayedFindObjectAddrCb(int rev, void *arg)
{
    text(log_fp, "\t [DelayedFindObjectAddrCb] \n");
    ReadWriteCtx *ctx = ( ReadWriteCtx *) arg;
    ctx->_this->alloc->FindObjectAddr(ctx->client_addr, FindCbRet, ctx );
}

// We expect that buffer will be allocated by reader module
void Worker::ReadCbRet(uint16_t actual_size,  addr_t newaddr, uint8_t version, void *owner)
{
    ReadWriteCtx *ctx = (ReadWriteCtx *)owner;
    if(actual_size == 0){
        ctx->retry++;
        if(ctx->retry < 3){
            // printf("retry read\n");
            info(log_fp, "\t\tSCHEDULE_CALLBACK DelayedFindObjectAddrCb\n");
            ctx->_this->CompleteSend((uint64_t)ctx->buffer); // hack
            ctx->buffer=0;
            if(newaddr == 0L){ // need to find object again
                mts::SCHEDULE_CALLBACK(ctx->_this->get_thread_id(), 0.001, DelayedFindObjectAddrCb, ctx); // delay because of compaction
            }else{
                mts::SCHEDULE_CALLBACK(ctx->_this->get_thread_id(), 0.001, DelayedFindObjectAddrCb, ctx); // delay because of compaction
                //mts::SCHEDULE_CALLBACK(ctx->_this->get_thread_id(), 0.4, DelayedReadCb, ctx); // delay because of lock
            }
            return;
        }
    }
    ctx->_this->ReadRet(actual_size, newaddr, version, ctx);
}

void Worker::WriteCbRet(addr_t newaddr, uint8_t success, uint8_t version, void *owner)
{
    ReadWriteCtx *ctx = (ReadWriteCtx *)owner;
    if(success == 0){
        ctx->retry++;
        if(ctx->retry < 3){
            if(newaddr == 0L){ // need to find object again
                mts::SCHEDULE_CALLBACK(ctx->_this->get_thread_id(), 1.4, DelayedFindObjectAddrCb, ctx); // delay because of compaction
            }else{
                mts::SCHEDULE_CALLBACK(ctx->_this->get_thread_id(), 0.05, DelayedFindObjectAddrCb, ctx); // delay because of compaction
                //mts::SCHEDULE_CALLBACK(ctx->_this->get_thread_id(), 0.4, DelayedWriteCb, ctx); // delay because of lock
            }
            return;
        } else {
            info(log_fp, "\t\tFailed after 2 retries WriteCbRet\n");
        }
    }
    
    ctx->_this->WriteRet(newaddr, success, version, ctx);
}

void Worker::AllocCbRet(client_addr_t client_addr, void *owner)
{
    AllocCtx *ctx = (AllocCtx *)owner;
    ctx->_this->AllocRet(client_addr, ctx);
}

// type can be removed
void Worker::FreeCbRet( int  status, addr_t newaddr, void *owner)
{
    FreeCtx *ctx = (FreeCtx *)owner;
    if(status == BLOCK_IS_NOT_FOUND) // retry
    {   
        ctx->retry++;
        if(ctx->retry < 3){
            mts::SCHEDULE_CALLBACK(ctx->_this->get_thread_id(), 2.4, DelayedFreeAddrCb, ctx);
            return;
        }
    }
 
    ctx->_this->FreeRet(status, newaddr, ctx);
}

void Worker::DelayedFreeAddrCb(int rev, void* owner)
{
    text(log_fp, "\t [DelayedFreeAddrCb] \n");
    FreeCtx *ctx = ( FreeCtx *) owner;
    AllocAdapter::getInstance().Free(ctx->_this->get_thread_id(), ctx->addr, ctx);
}

void Worker::ReadRet( uint32_t size,  addr_t newaddr, uint8_t version,  ReadWriteCtx *ctx)
{
    // prepare reply buffer
    uint32_t reply_size = sizeof(message_header_t) + sizeof(reply_t) + size;
 
    char *buffer = ctx->buffer;
    if(buffer == NULL){
       buffer = send_buffers.Alloc(reply_size);;
    }

    message_header_t *header = (message_header_t *)(buffer);
    header->thread_id = ctx->client;
    header->type = 0;

    reply_t *reply = (reply_t *)( buffer + sizeof(message_header_t)) ;
    text(log_fp, "\t\t\t[Worker(%u)] Send Read reply to request (%u) %" PRIu32 " with version %u ans size %u\n", local_thread_id, ctx->client, ctx->request_id, version,size);
    reply->id = ctx->request_id;
    reply->ret_addr.parts[0] = ctx->client_addr.parts[0];
    reply->ret_addr.parts[1] = ctx->client_addr.parts[1];
    reply->ret_addr.comp.addr = newaddr;
//    if(ctx->client_addr.comp.addr != newaddr) printf(">>>>>>>>>>>>>>>>>>>>>>>object has been moved\n");
    reply->status = (size == 0);
    reply->data_size = size;
    reply->version = version;

    // com module is responsible for mapping client ID to QP
    ctx->ep->send_signaled((uint64_t)buffer, (uint64_t)buffer, send_buffers.get_lkey(), reply_size);
    // free the read context
    delete ctx;
}


void Worker::WriteRet(addr_t newaddr, uint8_t success,  uint8_t version, ReadWriteCtx *ctx)
{
    // prepare reply buffer
    uint32_t reply_size = sizeof(message_header_t) + sizeof(reply_t);
    char *buffer = send_buffers.Alloc(reply_size);

    message_header_t *header = (message_header_t *)( buffer );
    header->thread_id = ctx->client;
    header->type = 0;

    reply_t *reply = (reply_t *)( buffer + sizeof(message_header_t) ) ;

    text(log_fp, "\t[Worker(%u)] Send Write reply to request  %" PRIu32 " with version %" PRIu8 " \n", local_thread_id, ctx->request_id, version);
    reply->id = ctx->request_id;
    reply->ret_addr.parts[0] = ctx->client_addr.parts[0];
    reply->ret_addr.parts[1] = ctx->client_addr.parts[1];
    reply->ret_addr.comp.addr = newaddr;
    reply->status = success;
    reply->data_size = 0;
    reply->version = version;

    ctx->ep->send_signaled((uint64_t)buffer, (uint64_t)buffer, send_buffers.get_lkey(), reply_size);
    post_recv_buf(ctx->wr_id);
    // free the write context
    delete ctx;
}




void Worker::AllocRet(client_addr_t ret_addr,  AllocCtx *ctx)
{
    // prepare reply buffer
    text(log_fp, "[AllocRet] parts %" PRIx64 " %" PRIx64 "\n", ret_addr.parts[0], ret_addr.parts[1]);
    uint32_t reply_size = sizeof(message_header_t) + sizeof(reply_t);
    char *buffer = send_buffers.Alloc(reply_size);

    message_header_t *header = (message_header_t *)( buffer );
    header->thread_id = ctx->client;
    header->type = 0;

    reply_t *reply = (reply_t *)( buffer + sizeof(message_header_t) ) ;

    text(log_fp, "\t[Worker(%u)] Send Alloc reply to request  %" PRIu32 "\n", local_thread_id, ctx->request_id);
    reply->id = ctx->request_id;
    reply->ret_addr.parts[0] = ret_addr.parts[0];
    reply->ret_addr.parts[1] = ret_addr.parts[1];
    reply->status = (ret_addr.comp.addr == 0); // use it for type
    reply->data_size = 0;
    reply->version = ret_addr.comp.version;

    text(log_fp, "\t[Worker(%u)] Send Alloc reply to thread id %u \n", local_thread_id, ctx->client);
    // com module is responsible for mapping client ID to QP/UD
    ctx->ep->send_signaled((uint64_t)buffer, (uint64_t)buffer, send_buffers.get_lkey(), reply_size);
    // free the write context
    delete ctx;
}




void Worker::FreeRet(int  status, addr_t newaddr,  FreeCtx *ctx)
{
    // prepare reply buffer
    uint32_t reply_size = sizeof(message_header_t) + sizeof(reply_t);
    char *buffer = send_buffers.Alloc(reply_size);

    message_header_t *header = (message_header_t *)( buffer );
    header->thread_id = ctx->client;
    header->type = 0;

    reply_t *reply = (reply_t *)( buffer + sizeof(message_header_t) ) ;

    text(log_fp, "\t[Worker(%u)] Send Free reply to request  %" PRIu32 "\n", local_thread_id, ctx->request_id);
    text(log_fp, "[FreeRet] parts %" PRIx64 " %" PRIx64 "\n", ctx->addr.parts[0], ctx->addr.parts[1]);
    reply->id = ctx->request_id;
    reply->ret_addr.parts[0] = 0;
    reply->ret_addr.parts[1] = 0;
    reply->ret_addr.comp.addr = newaddr;
    reply->status = (uint8_t)(-status);
    reply->data_size = 0;
    reply->version = 0;

    ctx->ep->send_signaled((uint64_t)buffer, (uint64_t)buffer, send_buffers.get_lkey(), reply_size);
    delete ctx;
}

void Worker::FixPointerCbRet(int ret, client_addr_t newaddr,  void *owner)
{
    FixPointerCtx *ctx = (FixPointerCtx *)owner;
    bool success = (ret == 0);
    if(ret < 0)
    {
//        info(log_fp, "Failed FixPointer. TODO. Implement RETRY with schedule callback");
        //mts::SCHEDULE_CALLBACK(local_thread_id, 0.4, DelayedFindObjectAddrCb, ctx);
    }
    ctx->_this->FixPointerRet(success, newaddr, ctx);

}


void Worker::FixPointerRet(uint8_t success, client_addr_t newaddr,  FixPointerCtx *ctx)
{
    uint32_t reply_size = sizeof(message_header_t) + sizeof(reply_t);
    char *buffer = send_buffers.Alloc(reply_size);

    message_header_t *header = (message_header_t *)( buffer );
    header->thread_id = ctx->client;
    header->type = 0;

    reply_t *reply = (reply_t *)( buffer + sizeof(message_header_t) ) ;

    text(log_fp, "\t[Worker(%u)] Send FixPointer reply to request  %" PRIu32 "\n", local_thread_id, ctx->request_id);
    reply->id = ctx->request_id;
    reply->ret_addr.parts[0] = newaddr.parts[0];
    reply->ret_addr.parts[1] = newaddr.parts[1];
    reply->status = success;
    reply->data_size = 0;

    ctx->ep->send_signaled((uint64_t)buffer, (uint64_t)buffer, send_buffers.get_lkey(), reply_size);
    delete ctx;
}


// 2 callbacks from Alloc adapter
 
void AllocAdapter::AllocReplyCb( client_addr_t ret_addr, void *owner)
{
    Worker::AllocCtx *allocctx = (Worker::AllocCtx * )owner;
    allocctx->cb(ret_addr, owner);
    AllocAdapter::getInstance().processAllocReply(ret_addr);
}


void AllocAdapter::FreeReplyCb(int ret, addr_t newaddr, void *owner)
{
    Worker::FreeCtx *ctx = (Worker::FreeCtx * )owner;
    uint8_t type = ctx->addr.comp.type;
    ctx->cb(ret, newaddr, owner);
    AllocAdapter::getInstance().processFreeReply(ret, type);
}
