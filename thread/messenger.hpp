/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * An implementation of a communication between threads.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */
#pragma once

#define PAYLOAD_SIZE 7 // no more than 7 pointers as a payload.

#include <thread>
#include <vector>
#include <cstdlib>
#include <cassert>
#include <boost/lockfree/queue.hpp>
#include <mutex>
#include <atomic>
#include <ev++.h>

 
namespace mts
{
 
    typedef void (*thread_delay_cb)( int revents, void *arg);

    struct thread_msg_t{
        typedef void (*thread_msg_cb)(thread_msg_t *msg);
        thread_msg_cb cb;
        void* payload[PAYLOAD_SIZE]; // PAYLOAD_SIZE Can be avoided by using circular memory pool for objects. 
        thread_msg_t() {}
    };
    static_assert(sizeof(thread_msg_t) == 64, "size of thread_msg_t is incorrect");

    thread_local uint8_t thread_id; 

    uint32_t num_threads;
    std::mutex reg_mutex;
    uint32_t reged_threads; 

    std::vector<boost::lockfree::queue<thread_msg_t*>*> msg_queues;
    std::vector<ev::async*> notifies;
    std::vector<ev::dynamic_loop*> loops;
    std::vector<void*> workers;


    void* GetWorker(uint32_t id){
        assert(reged_threads==num_threads);
        return workers[id];
    }

    void SetWorker(uint32_t id, void* worker){
        workers[id] = worker;
    }

     
    void send_msg_to_thread(uint32_t dst, thread_msg_t* msg){
        assert(reged_threads==num_threads);
        msg_queues[dst]->push(msg);
    }

 

    void send_msg_to_thread_and_notify(uint32_t dst, thread_msg_t* msg){
        assert(reged_threads==num_threads);
        msg_queues[dst]->push(msg);
        notifies[dst]->send();
    }


    bool poll_receive(uint32_t thread_id, thread_msg_t** msg){
        return msg_queues[thread_id]->pop(*msg);
    }


    void setup_threads(uint32_t _num_threads){
        num_threads = _num_threads;
        reged_threads = 0;
        msg_queues.resize(num_threads);
        notifies.resize(num_threads);
        loops.resize(num_threads);
        workers.resize(num_threads);
    };

} 